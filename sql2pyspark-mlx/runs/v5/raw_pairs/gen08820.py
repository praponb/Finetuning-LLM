from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([23, 38]) & (F.col("product_id").isin([20, 30, 40]) | (F.col("unit_price") < 390)))
    .groupBy(
        F.when(F.col("quantity") >= 7, "high").when(F.col("quantity") >= 2, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(F.max("quantity").alias("max_quantity"))
)
