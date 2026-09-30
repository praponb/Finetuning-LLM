from pyspark.sql import functions as F

matched_keys = (
    df_order_items
    .select("product_id")
    .intersect(
        df_order_items
        .filter(F.col("product_id").isNull())
        .select("product_id")
    )
)

kept_rows = (
    df_order_items
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

with_dims = (
    kept_rows
    .alias("e")
    .join(df_orders.alias("o"), F.col("e.order_id") == F.col("o.id"), "left")
    .select("e.*", F.col("o.discount"))
)

valid = (
    with_dims
    .filter(F.col("product_id").isNotNull() & (F.col("quantity") >= 8))
    .select("*", F.round((F.col("discount") * 100), 2).alias("discount_scaled"))
)

report = (
    valid
    .select("id", "order_id", "product_id", "quantity", "discount", "discount_scaled")
)

result = (
    report
    .select("id", "order_id", "quantity", "discount")
    .orderBy("quantity")
)
