from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.upper(F.trim("product_name")) == "USB-C HUB") & F.array_contains("tags", "new"))
    .select(
        "id",
        "stock_qty",
        "brand",
        "product_name",
        F.length("category").alias("category_length"),
        F.last_day("launch_date").alias("launch_date_month_end"),
    )
)
