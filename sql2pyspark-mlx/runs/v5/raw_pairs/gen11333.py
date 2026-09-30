from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("quantity") >= 7) | (F.col("quantity") >= 4))
    .groupBy("order_id")
    .agg(
        F.sum((F.col("quantity") * F.col("unit_price"))).alias("revenue"),
        F.count("*").alias("cnt"),
        F.max("unit_price").alias("max_unit_price"),
    )
    .filter(F.col("revenue") > 54)
    .orderBy(F.col("cnt").desc(), "revenue")
)
