from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("device").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("browser").orderBy(F.col("signup_date").desc_nulls_last(), "id")

base = (
    df_sessions
    .filter((F.col("country").eqNullSafe(F.lit("UK"))) & F.col("browser").isNotNull())
    .withColumn("browser_group", F.when(F.col("browser").isin(["Firefox", "Edge"]), "Tier 1").otherwise("Other"))
    .withColumn("browser_device", F.concat("browser", F.lit(" "), "device"))
)

enriched = (
    base
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id") == F.col("p.id"), "inner")
    .select(
        "src.*",
        F.col("p.country").alias("customer_country"),
        F.col("p.signup_date"),
        F.col("p.city"),
    )
)

with_windows = (
    enriched
    .filter(F.col("device").isin(["tablet", "desktop", "mobile"]))
    .select("*", F.last("city", ignorenulls=True).over(w_hist).alias("last_known_city"))
)

with_last = (
    with_windows
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") <= F.col("s.signup_date")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.duration_sec"))
)

deduped = (
    with_last
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
)

summary = (
    deduped
    .rollup("country")
    .agg(
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("duration_min"),
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("duration_min_2"),
        F.round(F.sum("duration_sec") / F.nullif(F.count(F.when(F.col("device") == "desktop", 1)), F.lit(0)), 2).alias("duration_sec_per_desktop"),
        F.grouping("country").alias("grouping_level"),
    )
)

result = (
    summary
    .select("duration_sec_per_desktop")
    .orderBy(F.col("duration_sec_per_desktop").asc_nulls_last())
)
