from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("product_id")
    .agg(
        F.max("units").alias("max_units"),
        F.count("*").alias("store_sale_count"),
    )
    .filter(F.col("max_units") > 20)
    .select("product_id", "max_units", "store_sale_count")
)
