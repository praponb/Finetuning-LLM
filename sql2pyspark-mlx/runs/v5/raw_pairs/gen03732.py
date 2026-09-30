from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id").isin([8, 21, 26, 36]) & (F.col("cost") > 640))
    .agg(
        F.round(F.avg("price"), 2).alias("avg_price"),
        F.sum("price").alias("total_price"),
    )
    .orderBy("avg_price")
)
