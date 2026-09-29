from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("sale_date") >= "2025-03-05")
    .select("sale_date")
    .intersect(df_subscriptions.select(F.col("end_date").alias("sale_date")))
    .orderBy("sale_date")
)
