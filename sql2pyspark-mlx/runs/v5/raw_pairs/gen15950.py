from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(F.date_format("sale_date", "yyyy-MM").alias("sale_month"))
    .agg(F.avg("units").alias("avg_units"))
    .filter(F.col("avg_units") >= 22)
    .orderBy("sale_month")
)
