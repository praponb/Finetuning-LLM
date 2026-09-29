from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter(~F.col("c.country").isin(["UK", "JP", "TH"]))
    .select(
        F.col("o.store_id"),
        F.col("o.shipping_fee"),
        F.col("o.order_date"),
        F.col("c.is_active"),
        F.col("c.name"),
        F.col("c.age"),
    )
    .orderBy(F.col("shipping_fee").desc())
)
