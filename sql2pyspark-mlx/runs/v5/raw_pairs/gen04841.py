from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.lower(F.trim("product_name")) == "running shoes")
    .select(
        "id",
        "tags",
        "cost",
        "brand",
        (F.col("price") / F.nullif("stock_qty", F.lit(0))).alias("price_per_stock_qty"),
    )
)
