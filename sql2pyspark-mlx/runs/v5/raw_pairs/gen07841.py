from pyspark.sql import functions as F

result = (
    df_products
    .select(
        "id",
        "stock_qty",
        "supplier_id",
        F.month("launch_date").alias("launch_date_month"),
    )
)
