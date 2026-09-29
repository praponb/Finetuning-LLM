from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price").between(190, 220) & F.col("quantity").between(3, 6))
    .groupBy("order_id")
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.count("quantity").alias("quantity_count"),
    )
    .orderBy(F.col("quantity_count").desc())
)
