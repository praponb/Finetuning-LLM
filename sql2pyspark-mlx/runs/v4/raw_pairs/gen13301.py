from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("monthly_fee") < 90) & (F.col("monthly_fee") <= 210))
    .groupBy("status")
    .agg(
        F.max("start_date").alias("last_start_date"),
        F.count("monthly_fee").alias("monthly_fee_count"),
    )
    .filter(F.col("monthly_fee_count") >= 3)
    .orderBy("last_start_date", F.col("status").desc())
)
