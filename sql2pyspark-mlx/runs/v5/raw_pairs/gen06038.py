from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue").between(2500, 2700))
    .groupBy("product_id")
    .agg(
        F.count("*").alias("store_sale_count"),
        F.max("units").alias("max_units"),
    )
    .orderBy("product_id")
    .limit(5)
)
