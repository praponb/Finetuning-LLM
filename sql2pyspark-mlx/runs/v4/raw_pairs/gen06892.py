from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(F.month("sale_date").alias("sale_month"))
    .agg(
        F.count("*").alias("store_sale_count"),
        F.countDistinct("units").alias("unique_products"),
        F.max("units").alias("max_units"),
    )
    .orderBy(F.col("sale_month").desc(), F.col("max_units").desc())
)
