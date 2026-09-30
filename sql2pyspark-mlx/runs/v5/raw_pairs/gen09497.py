from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(F.year("end_date").alias("end_year"))
    .agg(
        F.count("monthly_fee").alias("monthly_fee_count"),
        F.count("*").alias("total_count"),
    )
    .filter(F.col("total_count") >= 3)
)
