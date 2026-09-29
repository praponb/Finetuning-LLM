from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(F.year("sale_date").alias("sale_year"))
    .agg(F.max("sale_date").alias("last_sale_date"))
    .orderBy(F.col("sale_year").desc())
)
