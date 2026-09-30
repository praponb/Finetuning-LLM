from pyspark.sql import functions as F

result = (
    df_products
    .select(
        "id",
        "tags",
        "launch_date",
        F.last_day("launch_date").alias("launch_date_month_end"),
    )
    .orderBy("launch_date", "id")
    .limit(10)
)
