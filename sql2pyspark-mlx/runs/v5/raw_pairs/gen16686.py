from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("started_at", "id")
w2 = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_sessions
    .filter((F.col("started_at") >= F.lit("2023-05-27 01:00:00").cast("timestamp")) & F.col("browser").isNotNull())
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        F.coalesce("country", F.lit("Unknown")).alias("country_filled"),
    )
)

with_dims = (
    cleaned
    .alias("a")
    .join(df_customers.alias("b"), F.col("a.customer_id") == F.col("b.id"), "left")
    .select(
        "a.id",
        "a.customer_id",
        "a.started_at",
        F.col("b.credit_limit"),
        F.col("b.signup_date"),
        F.col("b.name"),
    )
)

with_gaps = (
    with_dims
    .withColumn("new_group_flag", F.when(F.col("started_at") - F.lag("started_at").over(w) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

islands = with_gaps.withColumn("island_id", F.sum("new_group_flag").over(w2))

remaining_keys = (
    islands
    .select("customer_id", "id")
    .exceptAll(
        df_sessions
        .filter(F.col("customer_id").isNull())
        .select("customer_id", "id")
    )
)

verified = (
    islands
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

with_dims_sessions = (
    verified
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .select(
        "a.customer_id",
        "a.credit_limit",
        "a.signup_date",
        "a.name",
        "a.new_group_flag",
        "a.island_id",
        F.col("c.segment"),
        F.col("c.city"),
    )
)

enriched = (
    with_dims_sessions
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id").eqNullSafe(F.col("p.id")), "left")
    .filter(F.col("p.segment") == "Corporate")
    .select(
        "e.*",
        F.col("p.city").alias("customer_city"),
        F.col("p.age"),
        F.col("p.email"),
    )
)

cleaned_sessions = (
    enriched
    .filter(F.month("signup_date").isin([2, 9, 12]))
    .select("*", F.lower("name").alias("name_lower"))
)

result = (
    cleaned_sessions
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id") == F.col("d.id"), "inner")
    .groupingSets(
        [["customer_city", "session_customer_city"], ["customer_city"], []],
        F.col("d.city").alias("customer_city"),
        F.col("a.customer_city").alias("session_customer_city"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.min(F.col("a.island_id")).alias("min_island_id"),
        F.grouping_id().alias("gid"),
    )
)
