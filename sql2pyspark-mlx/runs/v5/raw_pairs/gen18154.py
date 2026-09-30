from pyspark.sql import functions as F

filtered = (
    df_products
    .filter((F.col("launch_date") >= F.lit("2023-04-06").cast("date")) & (~F.col("brand").eqNullSafe(F.lit("Initech"))))
    .select(
        "id",
        "category",
        "brand",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        F.lower("category").alias("category_lower"),
        F.array_contains("tags", "premium").alias("is_premium"),
        F.coalesce("brand", F.lit("UNKNOWN")).alias("brand_clean"),
    )
)

result = (
    filtered
    .groupBy("supplier_id")
    .pivot("category", ["Home", "Books"])
    .agg(F.sum("stock_qty"))
    .select("supplier_id", F.col("Home").alias("home"), F.col("Books").alias("books"))
)
