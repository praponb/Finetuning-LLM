from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("price") <= 680)
    .select(
        "category",
        "cost",
        "stock_qty",
        F.date_format("launch_date", "yyyy-MM").alias("launch_date_label"),
    )
)
