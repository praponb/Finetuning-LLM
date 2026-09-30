from pyspark.sql import functions as F

recent_store_sales = df_store_sales.filter(F.col("revenue") < 3400)

summary = (
    recent_store_sales
    .groupBy("store_id", "product_id")
    .agg(
        F.count("units").alias("units_count"),
        F.countDistinct("revenue").alias("unique_stores"),
    )
)

result = (
    summary
    .filter(F.col("units_count") < 9)
    .orderBy("unique_stores")
)
