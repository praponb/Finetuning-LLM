from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity").between(3, 4))
    .groupBy("order_id")
    .agg(
        F.count("quantity").alias("quantity_count"),
        F.count("unit_price").alias("unit_price_count"),
        F.sum("quantity").alias("total_quantity"),
    )
    .orderBy(F.col("quantity_count").desc(), F.col("order_id").desc())
    .limit(3)
)
