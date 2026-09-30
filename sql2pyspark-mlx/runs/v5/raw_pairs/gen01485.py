from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("product_name").like("%Offic%") & ~F.col("brand").isin(["Stark", "Initech", "Acme"]))
    .agg(F.avg("price").alias("avg_price"))
    .orderBy(F.col("avg_price").desc())
)
