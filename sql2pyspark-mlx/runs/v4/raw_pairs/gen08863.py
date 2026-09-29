from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price") <= 210)
    .groupBy(
        F.when(F.col("quantity") >= 7, "high").when(F.col("quantity") >= 3, "mid").otherwise("low").alias("quantity_bucket"),
        "order_id",
    )
    .agg(
        F.min("unit_price").alias("min_unit_price"),
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
    )
    .filter(F.col("min_unit_price") < 180)
    .orderBy(F.col("quantity_bucket").desc())
)
