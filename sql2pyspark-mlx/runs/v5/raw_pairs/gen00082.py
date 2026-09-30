from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price").isNotNull())
    .groupBy("product_id", "order_id")
    .agg(
        F.sum("unit_price").alias("total_unit_price"),
        F.sum((F.col("quantity") * F.col("unit_price"))).alias("revenue"),
    )
    .filter(F.col("revenue") > 23)
)
