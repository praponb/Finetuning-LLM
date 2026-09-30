from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("category", "brand")
    .agg(
        F.max("launch_date").alias("latest_launch_date"),
        F.avg("cost").alias("avg_cost"),
    )
    .filter(F.col("avg_cost") < 540)
    .select("category", "brand", "latest_launch_date", "avg_cost")
    .orderBy("brand", "category")
    .limit(50)
)
