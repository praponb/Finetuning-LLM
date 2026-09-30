from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([3, 7, 8]))
    .groupBy(
        F.year("sale_date").alias("sale_year"),
        F.quarter("sale_date").alias("sale_quarter"),
    )
    .agg(
        F.countDistinct("revenue").alias("unique_stores"),
        F.min("sale_date").alias("first_sale_date"),
        F.max("sale_date").alias("last_sale_date"),
    )
    .orderBy(F.col("sale_year").desc())
)
