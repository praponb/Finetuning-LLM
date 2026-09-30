from pyspark.sql import functions as F

valid = (
    df_inventory
    .filter((F.col("last_restock") >= F.to_date(F.lit("2023-04-23"))) & F.col("product_id").isin([4, 7]))
    .withColumn("last_restock_weekofyear", F.weekofyear("last_restock"))
)

joined = (
    valid
    .alias("a")
    .join(df_products.alias("b"), F.col("a.product_id") == F.col("b.id"), "inner")
    .filter(F.length(F.col("b.product_name")) >= 13)
    .select(
        "a.id",
        "a.store_id",
        "a.product_id",
        F.col("b.stock_qty"),
        F.col("b.product_name"),
    )
)

with_dims = (
    joined
    .alias("s")
    .join(df_stores.alias("b"), F.col("s.store_id") == F.col("b.id"), "left")
    .select(
        "s.id",
        "s.store_id",
        "s.product_id",
        "s.stock_qty",
        F.col("b.opened_date"),
        F.col("b.city"),
        F.col("b.sqft"),
    )
)

valid_inventory = (
    with_dims
    .filter(F.col("city").eqNullSafe(F.lit("Chiang Mai")))
    .select(
        "id",
        "product_id",
        "stock_qty",
        "opened_date",
        "city",
        "sqft",
        F.round("sqft", 1).alias("sqft_rounded"),
        F.coalesce("city", F.lit("NONE")).alias("resolved_city"),
    )
)

result = (
    valid_inventory
    .select(
        "id",
        "product_id",
        "stock_qty",
        "opened_date",
        "city",
        "sqft",
        "resolved_city",
    )
    .orderBy(F.col("product_id").asc_nulls_first())
)
