from pyspark.sql import functions as F

order_totals = (
    df_order_items
    .groupBy("order_id")
    .agg(
        F.countDistinct("quantity").alias("unique_products"),
        F.avg("unit_price").alias("avg_unit_price"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_totals.alias("ot"), F.col("o.id") == F.col("ot.order_id"), "left")
    .select(
        F.col("o.id"),
        F.col("o.customer_id"),
        F.col("ot.unique_products"),
        F.col("ot.avg_unit_price"),
    )
    .orderBy(F.col("customer_id").desc())
)
