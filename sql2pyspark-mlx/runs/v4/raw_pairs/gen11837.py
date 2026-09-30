from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(F.date_format("sale_date", "yyyy-MM").alias("sale_month"))
    .agg(F.countDistinct("revenue").alias("unique_stores"))
    .filter(F.col("unique_stores") >= 6)
    .select("sale_month", "unique_stores")
    .orderBy(F.col("unique_stores").desc())
)
