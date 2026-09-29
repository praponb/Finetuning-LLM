from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("product_id")
    .agg(F.countDistinct("units").alias("unique_stores"))
    .filter(F.col("unique_stores") > 9)
    .select("product_id", "unique_stores")
    .orderBy("product_id")
)
