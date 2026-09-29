from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter(F.upper(F.col("c.email")).like("%RAN%") & (F.col("o.order_date") >= "2024-02-01"))
    .select(
        F.col("o.id"),
        F.col("o.customer_id"),
        F.col("s.store_name"),
        F.col("s.opened_date"),
        F.col("c.city"),
        F.when(F.col("o.shipping_fee") >= 33, "High").when(F.col("o.shipping_fee") >= 12, "Medium").otherwise("Low").alias("shipping_fee_tier"),
    )
    .orderBy(F.col("customer_id").desc())
)
