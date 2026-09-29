from pyspark.sql import functions as F

filtered_store_sales = df_store_sales.filter((F.col("units") != 11) & (F.col("units") != 28))

agg = (
    filtered_store_sales
    .groupBy("product_id")
    .agg(
        F.countDistinct("units").alias("unique_products"),
        F.sum("units").alias("total_units"),
    )
)

result = agg.select("product_id", "unique_products", "total_units")
