from pyspark.sql import functions as F

result = (
    df_shipments.alias("shipments")
    .join(df_orders.filter(F.col("amount").between(870, 1320)).alias("orders"), F.col("shipments.order_id") == F.col("orders.id"), "left_semi")
    .select(
        F.col("shipments.id"),
        F.col("shipments.status"),
        F.col("shipments.carrier"),
    )
)
