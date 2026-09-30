from pyspark.sql import functions as F

cleaned = (
    df_products
    .filter(F.col("brand").isNotNull() & F.col("supplier_id").isin([2, 8, 30]))
    .select(
        "*",
        F.coalesce("product_name", F.lit("Unknown")).alias("product_name_filled"),
        F.coalesce("brand", F.lit("DIRECT")).alias("resolved_brand"),
    )
)

not_flagged = (
    cleaned
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.quantity") > 5), "left_anti")
)

matched_keys = (
    not_flagged
    .select("brand", "id")
    .exceptAll(
        df_products
        .filter(F.col("supplier_id").isin([22, 32, 36]))
        .select("brand", "id")
    )
)

kept_rows = (
    not_flagged
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.brand") == F.col("k.brand")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

kept_keys = (
    kept_rows
    .select("id", "category")
    .subtract(
        df_products
        .filter(F.size("tags") >= 2)
        .select("id", "category")
    )
)

reconciled = (
    kept_rows
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.category") == F.col("k.category")), "inner")
    .select("s.*")
)

kept = (
    reconciled
    .alias("s")
    .join(df_order_items.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.order_id").isin([2, 18, 20])), "left_anti")
)

result = (
    kept
    .select("category", "cost", "price")
    .unpivot(
        ids=["category"],
        values=["cost", "price"],
        variableColumnName="metric_name",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
