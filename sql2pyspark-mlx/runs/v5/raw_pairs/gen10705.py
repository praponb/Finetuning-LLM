from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy(F.col("rating").desc())
w2 = Window.partitionBy("brand").orderBy(F.col("rating").desc_nulls_first())
w3 = Window.partitionBy("country")

with_dims = (
    df_products
    .alias("src")
    .join(df_suppliers.alias("s"), F.col("src.supplier_id") == F.col("s.id"), "left")
    .select(
        "src.id",
        "src.product_name",
        "src.brand",
        "src.supplier_id",
        F.col("s.country"),
        F.col("s.supplier_name"),
        F.col("s.city"),
    )
)

not_flagged = (
    with_dims
    .alias("s")
    .join(df_inventory.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.store_id") == 1), "left_anti")
)

not_flagged_products = (
    not_flagged
    .alias("s")
    .join(df_order_items.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.product_id").isNull()), "left_semi")
)

enriched = (
    not_flagged_products
    .alias("e")
    .join(df_suppliers.alias("b"), F.col("e.supplier_id") == F.col("b.id"), "left")
    .filter(F.col("b.rating") >= 4.0)
    .select(
        "e.*",
        F.col("b.country").alias("supplier_country"),
        F.col("b.supplier_name").alias("supplier_supplier_name"),
        F.col("b.rating"),
    )
)

remaining_keys = (
    enriched
    .select("id", "supplier_id")
    .intersectAll(
        df_products
        .filter(F.col("cost") >= 570)
        .select("id", "supplier_id")
    )
)

kept_rows = (
    enriched
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.supplier_id") == F.col("k.supplier_id")), "inner")
    .select("s.*")
)

remaining_keys_2 = (
    kept_rows
    .select("id")
    .intersect(
        df_products
        .filter(F.col("category") == "Fashion")
        .select("id")
    )
)

verified = (
    kept_rows
    .alias("s")
    .join(remaining_keys_2.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

scored = (
    verified
    .select(
        "*",
        F.dense_rank().over(w).alias("rating_dense_rank"),
        F.cume_dist().over(w2).alias("rating_cume_dist"),
        F.round((F.col("rating") / F.sum("rating").over(w3)), 4).alias("rating_share"),
    )
)

result = (
    scored
    .groupBy("city", "supplier_id")
    .pivot("country", ["DE", "US", "JP"])
    .agg(F.sum("rating_share").alias("amount"), F.count(F.lit(1)).alias("n"))
)
