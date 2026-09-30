from pyspark.sql import functions as F

cleaned = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "order_id",
        "product_id",
        "unit_price",
        F.col("unit_price").cast("int").alias("unit_price_int"),
    )
)

joined = (
    cleaned
    .alias("src")
    .join(df_products.alias("p"), F.col("src.product_id") == F.col("p.id"), "left")
    .select(
        "src.id",
        "src.order_id",
        "src.product_id",
        "src.unit_price",
        "src.unit_price_int",
        F.col("p.product_name"),
        F.col("p.price"),
        F.col("p.cost"),
    )
)

base = (
    joined
    .filter(F.col("price").between(560, 970))
    .select(
        "id",
        "product_id",
        "unit_price",
        "unit_price_int",
        "product_name",
        "price",
        "cost",
        F.coalesce("unit_price", F.lit(0)).alias("unit_price_filled"),
        F.greatest("unit_price_int", "cost").alias("greatest_unit_price_int_cost"),
    )
)

matched_keys = (
    base
    .select("product_id")
    .intersectAll(
        df_order_items
        .filter(F.col("order_id") == 24)
        .select("product_id")
    )
)

reconciled = (
    base
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

enriched = (
    reconciled
    .alias("src")
    .join(df_products.alias("p"), F.col("src.product_id") == F.col("p.id"), "left")
    .filter(F.size(F.col("p.tags")) >= 2)
    .select(
        "src.*",
        F.col("p.product_name").alias("product_product_name"),
        F.col("p.cost").alias("product_cost"),
    )
)

result = (
    enriched
    .select(
        "id",
        "product_id",
        "unit_price",
        "unit_price_int",
        "product_name",
        "price",
        "cost",
        "unit_price_filled",
        "greatest_unit_price_int_cost",
    )
    .orderBy(F.col("id").desc_nulls_last())
)
