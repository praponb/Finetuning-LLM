from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price").between(270, 380) | F.col("product_id").isNull())
    .groupBy(
        F.when(F.col("quantity") >= 8, "high").when(F.col("quantity") >= 3, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        F.sum("quantity").alias("total_quantity"),
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
    )
    .filter(F.col("total_quantity") > 41)
)
