from pyspark.sql import functions as F

result = (
    df_products
    .select(
        "id",
        "supplier_id",
        "category",
        "cost",
        (F.col("price") / F.nullif("stock_qty", F.lit(0))).alias("price_per_stock_qty"),
        F.concat_ws("/", "product_name", "category").alias("product_name_category_key"),
    )
    .orderBy("cost", "id")
    .limit(10)
)
