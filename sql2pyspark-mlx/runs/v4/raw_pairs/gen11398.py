from pyspark.sql import functions as F

result = (
    df_products
    .select(
        "id",
        "cost",
        "supplier_id",
        "tags",
        F.date_format("launch_date", "yyyy-MM-dd").alias("launch_date_label"),
    )
)
