from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("units") < 25) & (F.col("units") != 29))
    .groupBy("product_id", "store_id")
    .agg(F.max("sale_date").alias("last_sale_date"))
)
