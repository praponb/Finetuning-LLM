from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.month("launch_date").isin([6, 10, 11]) & ((F.col("brand") == "Umbrella") | (F.col("launch_date") >= "2025-05-01")))
    .groupBy("category")
    .agg(F.max("cost").alias("max_cost"))
    .filter(F.col("max_cost") >= 520)
)
