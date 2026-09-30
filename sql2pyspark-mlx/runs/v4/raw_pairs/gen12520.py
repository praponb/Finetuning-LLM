from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .filter(F.col("o.customer_id").isin([30, 35, 39]) & (F.col("s.carrier") == "FedEx"))
    .select(
        F.col("s.cost"),
        F.col("o.store_id"),
        F.col("o.shipping_fee"),
        F.col("o.order_date"),
    )
)
