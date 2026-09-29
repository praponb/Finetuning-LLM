from pyspark.sql import functions as F

base = (
    df_products
    .filter(F.col("price") < 330)
    .select(
        "id",
        "product_name",
        "category",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        F.array_join("tags", "|").alias("tags_text"),
    )
)

result = (
    base
    .select("supplier_id", "category")
    .subtract(
        df_products
        .filter(F.col("price") >= 950)
        .select("supplier_id", "category")
    )
)
