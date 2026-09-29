from pyspark.sql import functions as F

base = (
    df_order_items
    .filter(F.col("product_id").isin([26, 34, 39]))
    .withColumn("line_total", (F.col("quantity") - F.col("unit_price")))
)

agg = (
    base
    .groupBy("product_id")
    .agg(
        F.countDistinct("line_total").alias("unique_orders"),
        F.sum("unit_price").alias("total_unit_price"),
    )
)

result = (
    agg
    .select("product_id", "unique_orders", "total_unit_price")
    .orderBy("product_id", "unique_orders")
)
