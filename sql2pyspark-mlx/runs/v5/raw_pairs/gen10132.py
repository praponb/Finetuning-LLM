from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("started_at", "id")
w2 = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_sessions
    .withColumn("new_group_flag", F.when(F.lag("started_at").over(w).isNull() | (F.col("started_at") > F.lag("started_at").over(w) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

islands = with_gaps.withColumn("session_no", F.sum("new_group_flag").over(w2))

kept_keys = (
    islands
    .select("id", "country")
    .subtract(
        df_sessions
        .filter(F.col("browser") == "Edge")
        .select("id", "country")
    )
)

kept_rows = (
    islands
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

filtered = (
    kept_rows
    .filter((F.col("started_at") >= F.lit("2023-04-09").cast("date")) & F.col("browser").isNotNull())
    .withColumn("browser_title", F.initcap(F.trim("browser")))
    .withColumn("device_upper", F.upper("device"))
)

base = (
    filtered
    .filter((F.col("started_at") > F.lit("2023-03-26").cast("date")) & F.col("country").isNotNull())
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        "session_no",
        "browser_title",
        "device_upper",
        F.when(F.col("country") == "TH", 1).otherwise(0).alias("is_th"),
        F.coalesce("browser", F.lit("DIRECT")).alias("browser_filled"),
    )
)

matched_keys = (
    base
    .select("browser", "id")
    .intersectAll(
        df_sessions
        .filter(F.col("device").isin(["mobile", "desktop", "tablet"]))
        .select("browser", "id")
    )
)

verified = (
    base
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.browser") == F.col("k.browser")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

with_dims = (
    verified
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id") == F.col("d.id"), "left")
    .select("src.*", F.col("d.segment"), F.col("d.name"), F.col("d.email"))
)

enriched = (
    with_dims
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id") == F.col("d.id"), "left")
    .select(
        "src.id",
        "src.customer_id",
        "src.device",
        "src.browser",
        "src.started_at",
        "src.browser_title",
        "src.is_th",
        "src.segment",
        "src.email",
        F.col("d.signup_date"),
        F.col("d.email").alias("customer_email"),
        F.col("d.age"),
    )
)

result = (
    enriched
    .withColumn("activity_month", F.date_trunc("MONTH", "started_at"))
    .groupingSets(
        [["customer_id", "activity_month", "segment"], ["customer_id", "activity_month"], []],
        "customer_id",
        "activity_month",
        "segment",
    )
    .agg(
        F.sort_array(F.collect_set("browser")).alias("browser_list"),
        F.size(F.collect_set("browser")).alias("num_browsers"),
        F.grouping("customer_id").alias("grouping_level"),
    )
    .filter(F.col("num_browsers") >= 1)
    .orderBy(F.col("activity_month").desc())
)
