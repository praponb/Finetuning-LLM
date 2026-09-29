from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .filter(F.col("o.amount").between(640, 1260) & F.col("s.carrier").isin(["UPS", "FedEx", "Flash"]))
    .select(
        F.col("s.carrier"),
        F.col("o.shipping_fee"),
        F.col("o.store_id"),
        F.col("o.customer_id"),
    )
)
