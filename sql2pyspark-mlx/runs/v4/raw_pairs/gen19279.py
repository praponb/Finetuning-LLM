from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue").between(3300, 3900) & (F.year("sale_date") == 2025))
    .agg(
        F.count("*").alias("total_count"),
        F.countDistinct("units").alias("unique_products"),
    )
    .orderBy(F.col("unique_products").desc())
)
