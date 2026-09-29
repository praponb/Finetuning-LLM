from pyspark.sql import functions as F

valid = (
    df_products
    .filter(F.col("launch_date") > F.lit("2024-03-16").cast("date"))
    .select(
        "*",
        F.coalesce("product_name", F.lit("N/A")).alias("product_name_filled"),
        F.coalesce("brand", F.lit("OTHER")).alias("brand_filled"),
    )
)

with_dims = (
    valid
    .alias("src")
    .join(df_suppliers.alias("b"), F.col("src.supplier_id") == F.col("b.id"), "left")
    .select("src.*", F.col("b.country"), F.col("b.supplier_name"), F.col("b.rating"))
)

matched_keys = (
    with_dims
    .select("supplier_id", "id")
    .intersect(
        df_products
        .filter(F.col("supplier_id").isNull())
        .select("supplier_id", "id")
    )
)

verified = (
    with_dims
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.supplier_id") == F.col("k.supplier_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

grouped = (
    verified
    .groupBy(
        F.when(F.col("stock_qty") >= 335, "HIGH").otherwise("LOW").alias("stock_qty_tier"),
    )
    .agg(
        F.count(F.when(F.col("brand") == "Acme", 1)).alias("acme_rows"),
        F.round(F.sum("rating") / F.nullif(F.count(F.when(F.col("country") == "SG", 1)), F.lit(0)), 2).alias("rating_per_sg"),
        F.bit_and("stock_qty").alias("stock_qty_bit_and"),
        F.size(F.collect_set("brand")).alias("num_brands"),
        F.bit_and("stock_qty").alias("stock_qty_bit_and_2"),
        F.stddev_samp("price").alias("price_stddev"),
    )
    .filter(F.col("num_brands") < 1)
)

result = (
    grouped
    .select(
        "stock_qty_tier",
        "acme_rows",
        "rating_per_sg",
        "num_brands",
        "stock_qty_bit_and_2",
        "price_stddev",
    )
)
