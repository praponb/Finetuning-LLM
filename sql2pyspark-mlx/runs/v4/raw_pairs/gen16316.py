from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("product_id")
    .agg(
        F.countDistinct("revenue").alias("unique_stores"),
        F.min("units").alias("min_units"),
        F.count("revenue").alias("revenue_count"),
    )
    .orderBy(F.col("min_units").desc(), "product_id")
    .limit(1)
)
