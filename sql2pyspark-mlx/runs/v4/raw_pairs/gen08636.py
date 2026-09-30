from pyspark.sql import functions as F

result = (
    df_products
    .select(
        "id",
        "product_name",
        "cost",
        F.add_months("launch_date", 12).alias("launch_date_plus_months"),
    )
)
