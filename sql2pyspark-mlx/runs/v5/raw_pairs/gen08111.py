from pyspark.sql import functions as F

result = (
    df_subscriptions
    .alias("e")
    .join(df_customers.alias("dim"), F.col("e.customer_id") == F.col("dim.id"), "inner")
    .cube(
        F.col("dim.segment").alias("segment"),
        F.col("dim.country").alias("country"),
        F.col("e.status").alias("status"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.round(F.sum(F.col("e.monthly_fee")), 2).alias("total_monthly_fee"),
        F.grouping_id().alias("level_id"),
    )
)
