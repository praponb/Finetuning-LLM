from pyspark.sql import functions as F

filtered = (
    df_products
    .filter(F.col("price") <= 580)
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "tags",
        "supplier_id",
        F.concat("product_name", F.lit(" "), "category").alias("product_name_category"),
        F.size("tags").alias("tags_count"),
        F.coalesce("brand", F.lit("UNKNOWN")).alias("brand_filled"),
    )
)

result = (
    filtered
    .groupBy(
        "brand",
        F.when(F.col("tags_count") >= 73, "LARGE").otherwise("SMALL").alias("tags_count_tier"),
    )
    .agg(
        F.size(F.collect_set("category")).alias("num_categories"),
        F.round(F.avg("price"), 2).alias("avg_price"),
    )
    .filter(F.col("avg_price") < 1000000)
    .orderBy("avg_price")
)
