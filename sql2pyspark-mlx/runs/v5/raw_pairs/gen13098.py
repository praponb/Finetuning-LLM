from pyspark.sql import functions as F

result = (
    df_subscriptions
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .groupingSets(
        [["country", "status"], ["country"], []],
        F.col("c.country").alias("country"),
        F.col("a.status").alias("status"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.sum(F.col("a.monthly_fee")).alias("total_monthly_fee"),
        F.grouping_id().alias("grouping_level"),
    )
)
