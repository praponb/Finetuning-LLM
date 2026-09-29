from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .filter(F.col("o.discount").isNotNull() & F.col("s.carrier").isin(["Kerry", "FedEx", "DHL"]))
    .select(
        F.col("s.id"),
        F.col("s.carrier"),
        F.col("o.discount"),
        F.col("o.customer_id"),
    )
)
