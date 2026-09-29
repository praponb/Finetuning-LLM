from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.is_active"))
    .groupBy(F.col("c.is_active"))
    .agg(
        F.sum(F.when(F.col("o.status") == "SHIPPED", F.col("o.discount")).otherwise(0)).alias("shipped_discount"),
        F.max(F.col("o.amount")).alias("max_amount"),
    )
    .orderBy(F.col("max_amount").desc())
)
