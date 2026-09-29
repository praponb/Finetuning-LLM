from pyspark.sql import functions as F

product_totals = (
    df_order_items
    .filter(F.col("unit_price").isNull())
    .groupBy("product_id")
    .agg(
        F.avg("unit_price").alias("avg_unit_price"),
        F.countDistinct("quantity").alias("unique_orders"),
    )
)

result = (
    df_products.alias("p")
    .join(product_totals.alias("P"), F.col("p.id") == F.col("P.product_id"), "left")
    .filter(F.col("P.unique_orders") >= 4)
    .select(
        F.col("p.stock_qty"),
        F.col("p.launch_date"),
        F.col("P.avg_unit_price"),
        F.col("P.unique_orders"),
    )
    .orderBy("unique_orders")
)
