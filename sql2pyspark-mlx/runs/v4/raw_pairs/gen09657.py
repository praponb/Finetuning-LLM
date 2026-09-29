from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isin([4, 30]) & ((F.col("quantity") == 4) | F.col("order_id").isin([16, 30, 36])))
    .groupBy(
        F.when(F.col("quantity") >= 7, "high").when(F.col("quantity") >= 3, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.sum("unit_price").alias("total_unit_price"),
    )
    .orderBy(F.col("total_unit_price").desc())
)
