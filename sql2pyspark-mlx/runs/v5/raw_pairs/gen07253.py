from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id").isin([15, 27, 33, 40]))
    .groupBy(F.quarter("end_date").alias("end_quarter"))
    .agg(
        F.sum(F.when(F.col("plan") == "enterprise", 1).otherwise(0)).alias("enterprise_count"),
    )
)
