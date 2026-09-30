from pyspark.sql import functions as F

result = (
    df_products
    .select(
        "id",
        "launch_date",
        "category",
        F.date_trunc("year", "launch_date").alias("launch_date_period"),
    )
)
