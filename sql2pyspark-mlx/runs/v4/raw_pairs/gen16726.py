from pyspark.sql import functions as F

shipments_by_order = (
    df_shipments
    .filter(F.col("order_id") == 37)
    .groupBy("order_id")
    .agg(F.max("cost").alias("max_cost"))
)

result = (
    df_orders.alias("o")
    .join(shipments_by_order.alias("sbo"), F.col("o.id") == F.col("sbo.order_id"), "inner")
    .filter(F.col("sbo.max_cost") > 73)
    .select(
        F.col("o.id"),
        F.col("o.order_date"),
        F.col("o.discount"),
        F.col("sbo.max_cost"),
    )
)
