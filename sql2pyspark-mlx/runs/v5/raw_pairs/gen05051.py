from pyspark.sql import functions as F

recent_order_items = (
    df_order_items
    .filter(((F.col("unit_price") > 310) & F.col("product_id").isNotNull()) | (F.col("quantity") >= 4))
)

product_id_summary = (
    recent_order_items
    .groupBy("product_id", "order_id")
    .agg(
        F.max("unit_price").alias("max_unit_price"),
        F.sum((F.col("unit_price") * F.col("quantity"))).alias("revenue"),
    )
)

result = (
    product_id_summary
    .filter(F.col("max_unit_price") >= 130)
    .orderBy(F.col("product_id").desc())
)
