from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(F.date_format("launch_date", "yyyy-MM").alias("launch_month"))
    .agg(F.max("cost").alias("max_cost"))
    .filter(F.col("max_cost") > 650)
    .select("launch_month", "max_cost")
    .orderBy(F.col("launch_month").desc())
)
