from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(F.quarter("sale_date").alias("sale_quarter"))
    .agg(F.countDistinct("units").alias("unique_products"))
    .filter(F.col("unique_products") < 5)
    .select("sale_quarter", "unique_products")
    .orderBy(F.col("unique_products").desc())
)
