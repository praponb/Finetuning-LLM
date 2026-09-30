from pyspark.sql import functions as F

valid = (
    df_subscriptions
    .filter((F.col("end_date") >= F.lit("2023-09-20").cast("date")) & (F.col("end_date") <= "2024-08-01"))
    .withColumn("monthly_fee_adjusted", F.round((F.col("monthly_fee") * 1.1), 2))
    .withColumn("start_date_plus_months", F.add_months("start_date", 1))
)

result = (
    valid
    .alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .rollup(F.col("c.segment").alias("segment"), F.col("s.plan").alias("plan"))
    .agg(
        F.count("*").alias("num_rows"),
        F.avg(F.col("s.monthly_fee_adjusted")).alias("avg_monthly_fee_adjusted"),
        F.max(F.col("s.monthly_fee")).alias("max_monthly_fee"),
    )
)
