from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id")
w2 = Window.partitionBy("status")
w3 = Window.partitionBy("customer_id", "plan")
w_running = Window.partitionBy("country", "customer_id")
w5 = Window.partitionBy("customer_id").orderBy(F.col("min_monthly_fee_share_2").desc())

filtered = (
    df_subscriptions
    .filter(F.col("end_date") > F.to_date(F.lit("2023-06-29")))
    .select("*", F.lower("plan").alias("plan_lower"))
)

kept_keys = (
    filtered
    .select("customer_id")
    .exceptAll(
        df_subscriptions
        .filter(F.col("customer_id").isin([1, 40]))
        .select("customer_id")
    )
)

verified = (
    filtered
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

joined = (
    verified
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.country"), F.col("p.name"))
)

scored = (
    joined
    .withColumn("monthly_fee_share", F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w)), 4))
    .withColumn("customer_id_max_monthly_fee", F.max("monthly_fee").over(w))
    .withColumn("monthly_fee_share_2", F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w)), 4))
    .withColumn("status_count_monthly_fee", F.count("monthly_fee").over(w2))
)

metrics = (
    scored
    .select(
        "*",
        F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w3)), 4).alias("monthly_fee_share_3"),
        F.min("status_count_monthly_fee").over(w_running).alias("country_min_status_count_monthly_fee"),
    )
)

result = (
    metrics
    .groupBy("customer_id", "status")
    .agg(
        F.count(F.when(F.col("country") == "JP", 1)).alias("jp_count"),
        F.count("*").alias("event_count"),
        F.stddev_samp("monthly_fee").alias("monthly_fee_stddev"),
        F.round(F.sum("monthly_fee_share_3") / F.nullif(F.count(F.when(F.col("country") == "SG", 1)), F.lit(0)), 2).alias("monthly_fee_share_3_per_sg"),
        F.min("monthly_fee_share_2").alias("min_monthly_fee_share_2"),
        F.avg(F.when(F.col("country") == "US", F.col("monthly_fee_share_2"))).alias("us_avg_monthly_fee_share_2"),
    )
    .filter(F.col("monthly_fee_stddev") > 0)
    .withColumn("min_monthly_fee_share_2_rank", F.rank().over(w5))
    .orderBy("monthly_fee_stddev")
)
