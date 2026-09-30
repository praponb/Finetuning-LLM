from pyspark.sql import functions as F

order_summary = (
    df_order_items
    .filter(F.col("product_id").isin([1, 22, 32, 33]))
    .groupBy("order_id")
    .agg(
        F.countDistinct("unit_price").alias("unique_orders"),
        F.count("*").alias("num_order_items"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_summary.alias("t"), F.col("o.id") == F.col("t.order_id"), "inner")
    .filter(F.col("t.num_order_items") > 8)
    .select(
        F.col("o.id"),
        F.col("o.customer_id"),
        F.col("o.order_date"),
        F.col("t.unique_orders"),
        F.col("t.num_order_items"),
    )
    .orderBy("customer_id", "id")
    .limit(20)
)
