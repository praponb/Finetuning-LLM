from pyspark.sql import functions as F

base = (
    df_products
    .filter(F.col("category").isin(["Sports", "Home", "Fashion"]))
    .withColumn("sorted_tags", F.sort_array("tags"))
)

agg = (
    base
    .groupBy(F.date_format("launch_date", "yyyy-MM").alias("launch_month"))
    .agg(F.round(F.avg("cost"), 2).alias("avg_cost"))
)

result = (
    agg
    .filter(F.col("avg_cost") > 56)
    .select("launch_month", "avg_cost")
    .orderBy("launch_month")
)
