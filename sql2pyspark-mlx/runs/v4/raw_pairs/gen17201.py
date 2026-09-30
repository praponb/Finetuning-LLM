from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .groupBy(
        F.when(F.col("c.country").isin(["SG", "TH"]), "priority").otherwise("other").alias("country_group"),
        F.col("c.segment"),
    )
    .agg(
        F.sum(F.col("o.discount")).alias("total_discount"),
        F.count(F.col("o.shipping_fee")).alias("shipping_fee_count"),
    )
    .orderBy(F.col("segment").desc())
)
