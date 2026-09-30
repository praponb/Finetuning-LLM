from pyspark.sql import functions as F

filtered_products = (
    df_products
    .filter(F.month("launch_date").isin([5, 10, 11]) & (F.col("cost") >= 650))
    .withColumn("brand_title", F.initcap(F.trim("brand")))
)

agg = (
    filtered_products
    .groupBy("category")
    .agg(
        F.max("price").alias("max_price"),
        F.sum(F.when(F.col("brand") == "Stark", F.col("price")).otherwise(0)).alias("stark_price"),
    )
)

result = agg.select("category", "max_price", "stark_price")
