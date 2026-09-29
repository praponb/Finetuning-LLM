from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity").between(6, 7))
    .groupBy("product_id")
    .agg(F.sum((F.col("unit_price") * F.col("quantity"))).alias("revenue"))
    .filter(F.col("revenue") > 53)
    .select("product_id", "revenue")
    .orderBy(F.col("revenue").desc())
)
