from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("store_id")
    .agg(F.countDistinct("revenue").alias("unique_products"))
    .filter(F.col("unique_products") > 5)
    .select("store_id", "unique_products")
)
