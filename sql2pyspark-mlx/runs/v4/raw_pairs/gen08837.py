from pyspark.sql import functions as F

cleaned = (
    df_products
    .filter(F.col("brand").isNotNull())
    .withColumn("brand_category_key", F.concat_ws("/", "brand", "category"))
)

result = (
    cleaned
    .groupBy("supplier_id")
    .pivot("brand", ["Initech", "Umbrella"])
    .agg(F.sum("cost").alias("total"))
)
