from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("credit_limit").desc_nulls_last())
w2 = Window.partitionBy("customer_country").orderBy("credit_limit")
w3 = Window.partitionBy("segment", "customer_country").orderBy(F.col("credit_limit").desc_nulls_first())
w4 = Window.partitionBy("segment")
w5 = Window.partitionBy("segment").orderBy("credit_limit_cume_dist_2")
w6 = Window.partitionBy("year_start").orderBy(F.col("distinct_browsers").desc())

filtered = (
    df_sessions
    .filter(F.col("started_at") >= F.lit("2023-04-03 17:00:00").cast("timestamp"))
    .withColumn("device_upper", F.upper("device"))
    .withColumn("started_at_date", F.to_date("started_at"))
    .withColumn("resolved_country", F.coalesce("country", F.lit("NONE")))
)

enriched = (
    filtered
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.name"))
)

remaining_keys = (
    enriched
    .select("customer_id", "device")
    .intersect(
        df_sessions
        .filter(F.col("browser").isin(["Edge", "Safari"]))
        .select("customer_id", "device")
    )
)

reconciled = (
    enriched
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.device") == F.col("k.device")), "inner")
    .select("s.*")
)

with_dims = (
    reconciled
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id") == F.col("d.id"), "left")
    .select(
        "src.*",
        F.col("d.name").alias("customer_name"),
        F.col("d.country").alias("customer_country"),
    )
)

enriched_sessions = (
    with_dims
    .alias("e")
    .join(df_customers.alias("c"), F.col("e.customer_id") == F.col("c.id"), "left")
    .select("e.*", F.col("c.segment"), F.col("c.credit_limit"))
)

with_windows = (
    enriched_sessions
    .withColumn("credit_limit_cume_dist", F.cume_dist().over(w))
    .withColumn("credit_limit_cume_dist_2", F.cume_dist().over(w2))
    .withColumn("credit_limit_cume_dist_3", F.cume_dist().over(w3))
)

with_windows_sessions = (
    with_windows
    .withColumn("segment_sum_credit_limit_cume_dist_2", F.sum("credit_limit_cume_dist_2").over(w4))
    .withColumn("credit_limit_cume_dist_2_cume_dist", F.cume_dist().over(w5))
    .withColumn("segment_sum_credit_limit_cume_dist_3", F.sum("credit_limit_cume_dist_3").over(w4))
)

result = (
    with_windows_sessions
    .groupBy(
        F.date_trunc("YEAR", "started_at_date").alias("year_start"),
        F.date_trunc("MONTH", "started_at_date").alias("activity_month"),
    )
    .agg(
        F.max("started_at").alias("last_started_at"),
        F.percentile_approx("credit_limit", 0.9).alias("credit_limit_p90"),
        F.avg(F.when(F.col("customer_country") == "US", F.col("credit_limit_cume_dist_2_cume_dist"))).alias("us_avg_credit_limit_cume_dist_2_cume_dist"),
        F.countDistinct("browser").alias("distinct_browsers"),
    )
    .filter(F.col("credit_limit_p90") > 0)
    .withColumn("distinct_browsers_rank", F.rank().over(w6))
)
