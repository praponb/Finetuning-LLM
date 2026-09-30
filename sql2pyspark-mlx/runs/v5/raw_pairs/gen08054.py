from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("category").isin(["Sports", "Books", "Home"]))
    .select(
        "supplier_id",
        "tags",
        (F.col("price") / F.nullif("stock_qty", F.lit(0))).alias("price_per_stock_qty"),
        F.concat("brand", F.lit(" - "), "product_name").alias("brand_product_name"),
    )
    .orderBy(F.col("price_per_stock_qty").desc())
)
