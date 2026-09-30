from pyspark.sql import functions as F

order_totals = (
    df_order_items
    .groupBy("order_id")
    .agg(
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
        F.sum((F.col("quantity") * F.col("unit_price"))).alias("revenue"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_totals.alias("ot"), F.col("o.id") == F.col("ot.order_id"), "inner")
    .select(
        F.col("o.id"),
        F.col("o.store_id"),
        F.col("ot.stddev_quantity"),
        F.col("ot.revenue"),
    )
    .orderBy(F.col("stddev_quantity").desc(), "id")
    .limit(20)
)
