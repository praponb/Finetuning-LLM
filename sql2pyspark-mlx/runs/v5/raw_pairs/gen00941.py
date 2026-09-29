from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(
        "category",
        F.date_format("launch_date", "yyyy-MM").alias("launch_month"),
        "brand",
    )
    .agg(
        F.count("*").alias("total_count"),
        F.min("launch_date").alias("first_launch_date"),
    )
    .orderBy("category", "first_launch_date")
)
