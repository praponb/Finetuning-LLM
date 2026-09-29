from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.year("sale_date") >= 2024)
    .groupBy("product_id")
    .agg(F.countDistinct("units").alias("unique_products"))
)
