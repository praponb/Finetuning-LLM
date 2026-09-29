from pyspark.sql import functions as F

result = (
    df_store_sales
    .agg(F.countDistinct("revenue").alias("unique_stores"))
    .orderBy("unique_stores")
)
