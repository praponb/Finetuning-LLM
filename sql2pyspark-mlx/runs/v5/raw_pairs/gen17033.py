from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy("product_id")
    .agg(
        F.count("*").alias("row_count"),
        F.max("unit_price").alias("max_unit_price"),
        F.sum((F.col("quantity") * F.col("unit_price"))).alias("revenue"),
    )
    .filter(F.col("revenue") < 57)
    .drop("revenue")
    .orderBy(F.col("row_count").desc(), "product_id")
    .limit(10)
)
