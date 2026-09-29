from pyspark.sql import functions as F

result = (
    df_products
    .select(
        "id",
        "price",
        "product_name",
        F.last_day("launch_date").alias("launch_date_month_end"),
    )
    .orderBy("product_name")
)
