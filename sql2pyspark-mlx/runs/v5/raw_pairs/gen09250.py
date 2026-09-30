from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        F.when(F.col("quantity") >= 9, "high").when(F.col("quantity") >= 4, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        F.sum("unit_price").alias("total_unit_price"),
        F.sum("quantity").alias("total_quantity"),
    )
    .filter(F.col("total_quantity") > 23)
    .orderBy(F.col("total_unit_price").desc())
)
