from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("store_id")
    .agg(
        F.countDistinct("units").alias("unique_stores"),
        F.count("*").alias("store_sale_count"),
    )
    .filter(F.col("store_sale_count") >= 7)
)
