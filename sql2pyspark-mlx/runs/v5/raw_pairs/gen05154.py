from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy("order_id")
    .agg(
        F.avg("unit_price").alias("avg_unit_price"),
        F.sum("quantity").alias("total_quantity"),
    )
    .filter(F.col("total_quantity") > 19)
    .drop("total_quantity")
    .orderBy(F.col("avg_unit_price").desc())
)
