from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.array_contains("tags", "bestseller"))
    .select(
        "id",
        "price",
        "cost",
        "stock_qty",
        F.coalesce("price", F.lit(0)).alias("price_filled"),
        F.concat("product_name", F.lit(" - "), "brand").alias("product_name_brand"),
    )
)
