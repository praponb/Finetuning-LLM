from pyspark.sql import functions as F

remaining_keys = (
    df_order_items
    .select("order_id", "id")
    .exceptAll(
        df_order_items
        .filter(F.col("quantity") < 6)
        .select("order_id", "id")
    )
)

reconciled = (
    df_order_items
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

valid = (
    reconciled
    .filter(F.col("product_id").isNotNull())
    .select("*", F.round((F.col("quantity") * 1.15), 2).alias("quantity_scaled"))
)

with_dims = (
    valid
    .alias("s")
    .join(df_products.alias("b"), F.col("s.product_id") == F.col("b.id"), "left")
    .filter(F.col("b.cost").between(570, 610))
    .select("s.*", F.col("b.cost"), F.col("b.price"), F.col("b.brand"))
)

filtered = (
    with_dims
    .filter(F.col("product_id").isNotNull() & F.col("order_id").isin([4, 7, 37]))
    .select(
        "product_id",
        "quantity",
        "unit_price",
        "cost",
        "price",
        (F.col("quantity") + F.col("price")).alias("quantity_plus_price"),
        F.initcap(F.trim("brand")).alias("brand_title"),
    )
)

result = (
    filtered
    .select("product_id", "cost")
    .orderBy(F.col("product_id").desc_nulls_last())
)
