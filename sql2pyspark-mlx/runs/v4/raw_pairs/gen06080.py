from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(F.year("end_date").alias("end_year"))
    .agg(
        F.sum(F.when(F.col("plan") == "standard", F.col("monthly_fee")).otherwise(0)).alias("standard_monthly_fee"),
        F.count("monthly_fee").alias("monthly_fee_count"),
    )
)
