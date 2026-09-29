from pyspark.sql import functions as F

order_summary = (
    df_order_items
    .filter(F.col("order_id").isin([18, 27]))
    .groupBy("order_id")
    .agg(F.min("unit_price").alias("min_unit_price"))
)

result = (
    df_orders.alias("o")
    .join(order_summary.alias("os"), F.col("o.id") == F.col("os.order_id"), "inner")
    .filter(F.col("os.min_unit_price") > 170)
    .select(
        F.col("o.id"),
        F.col("o.shipping_fee"),
        F.col("o.amount"),
        F.col("os.min_unit_price"),
    )
    .orderBy(F.col("shipping_fee").desc(), F.col("id").desc())
)
