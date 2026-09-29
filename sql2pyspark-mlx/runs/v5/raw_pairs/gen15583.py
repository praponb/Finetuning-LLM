from pyspark.sql import functions as F

filtered = (
    df_products
    .filter(F.col("brand").isNotNull() & F.col("supplier_id").isNull())
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        F.trim("brand").alias("clean_brand"),
        F.array_join("tags", "|").alias("tags_text"),
        F.coalesce("brand", F.lit("UNKNOWN")).alias("brand_filled"),
    )
)

result = (
    filtered
    .alias("s")
    .join(df_inventory.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.product_id") == 4), "left_semi")
)
