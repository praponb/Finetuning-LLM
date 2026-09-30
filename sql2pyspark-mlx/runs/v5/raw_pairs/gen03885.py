from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.array_contains("tags", "bestseller"))
    .groupBy("brand")
    .agg(F.round(F.avg("price"), 2).alias("avg_price"))
    .filter(F.col("avg_price") >= 41)
)
