from pyspark.sql import functions as F

valid = (
    df_subscriptions
    .filter((F.col("start_date") > F.lit("2023-05-28").cast("date")) & (F.col("monthly_fee") >= 240))
    .withColumn("plan_prefix", F.substring("plan", 1, 3))
)

result = (
    valid
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id") == F.col("p.id"), "inner")
    .rollup(
        F.col("p.segment").alias("segment"),
        F.col("p.city").alias("city"),
        F.col("src.plan").alias("plan"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.max(F.col("src.monthly_fee")).alias("max_monthly_fee"),
    )
    .orderBy(F.col("cnt").desc_nulls_last())
)
