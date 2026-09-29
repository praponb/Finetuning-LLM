from pyspark.sql import functions as F

base = (
    df_products
    .filter(F.col("supplier_id").isNotNull())
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "tags",
        F.last_day("launch_date").alias("launch_date_month_end"),
    )
)

result = (
    base
    .select("category", "id")
    .intersectAll(
        df_products
        .filter(F.col("cost") <= 450)
        .select("category", "id")
    )
)
