from pyspark.sql import functions as F

result = (
    df_products
    .select(
        "launch_date",
        F.year("launch_date").alias("launch_date_year"),
        F.size("tags").alias("tags_count"),
    )
    .orderBy("launch_date")
)
