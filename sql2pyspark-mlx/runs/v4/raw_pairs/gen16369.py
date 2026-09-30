from pyspark.sql import functions as F

base = (
    df_products
    .filter((F.col("launch_date") >= F.lit("2023-12-23").cast("date")) & F.col("brand").isNotNull() & F.array_contains("tags", "premium"))
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "tags",
        "supplier_id",
        F.floor("price").alias("price_whole"),
        F.coalesce("brand", F.lit("N/A")).alias("brand_filled"),
    )
)

result = (
    base
    .groupBy(
        F.when(F.col("price") >= 970, "PREMIUM").otherwise("STANDARD").alias("price_tier"),
    )
    .agg(
        F.corr("price", "cost").alias("corr_price_cost"),
        F.round(F.avg("price_whole"), 2).alias("avg_price_whole"),
        F.sum("price").alias("total_price"),
        F.round(F.avg("cost"), 2).alias("avg_cost"),
        F.corr("stock_qty", "price").alias("corr_stock_qty_price"),
        F.size(F.collect_set("category")).alias("num_categories"),
    )
    .filter(F.col("corr_price_cost") >= 0)
    .orderBy(F.col("corr_stock_qty_price").desc_nulls_first())
)
