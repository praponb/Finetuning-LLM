from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .groupBy(F.col("c.city"))
    .agg(
        F.max(F.col("o.discount")).alias("max_discount"),
        F.sum(F.when(F.col("o.channel") == "partner", 1).otherwise(0)).alias("partner_count"),
    )
)
