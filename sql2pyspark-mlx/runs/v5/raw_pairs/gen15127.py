from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("supplier_id", "brand").orderBy("launch_date", "id")
w2 = Window.partitionBy("supplier_id").orderBy("is_sports", "id")
w3 = Window.partitionBy("supplier_id").orderBy(F.col("stock_qty").desc_nulls_first())
w4 = Window.partitionBy("supplier_id")

base = (
    df_products
    .filter(F.col("brand").isNotNull() & (~F.col("brand").eqNullSafe(F.lit("Globex"))))
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        F.when(F.col("category") == "Sports", 1).otherwise(0).alias("is_sports"),
    )
)

with_dims = (
    base
    .alias("e")
    .join(df_suppliers.alias("b"), F.col("e.supplier_id").eqNullSafe(F.col("b.id")), "left")
    .select("e.*", F.col("b.supplier_name"))
)

scored = (
    with_dims
    .select(
        "id",
        "price",
        "stock_qty",
        "tags",
        "supplier_id",
        F.lead("price").over(w_hist).alias("next_price"),
        F.lag("price").over(w_hist).alias("prev_price"),
        F.lag("is_sports").over(w_hist).alias("prev_is_sports"),
        F.lead("is_sports").over(w_hist).alias("next_is_sports"),
        F.row_number().over(w_hist).alias("row_seq"),
        F.row_number().over(w2).alias("row_seq_2"),
    )
)

eligible = (
    scored
    .alias("s")
    .join(df_inventory.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.last_restock").between("2024-04-01", "2025-02-16")), "left_semi")
)

remaining_keys = (
    eligible
    .select("supplier_id", "id")
    .subtract(
        df_products
        .filter(F.year("launch_date") >= 2025)
        .select("supplier_id", "id")
    )
)

verified = (
    eligible
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.supplier_id") == F.col("k.supplier_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

remaining_keys_2 = (
    verified
    .select("id", "supplier_id")
    .subtract(
        df_products
        .filter(F.col("product_name").like("L%"))
        .select("id", "supplier_id")
    )
)

verified_products = (
    verified
    .alias("s")
    .join(remaining_keys_2.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.supplier_id") == F.col("k.supplier_id")), "inner")
    .select("s.*")
)

with_windows = (
    verified_products
    .select(
        "price",
        "stock_qty",
        "row_seq",
        F.cume_dist().over(w3).alias("stock_qty_cume_dist"),
        F.max("next_is_sports").over(w4).alias("supplier_id_max_next_is_sports"),
    )
)

summary = (
    with_windows
    .groupBy(
        F.when(F.col("stock_qty_cume_dist") >= 0.65, "HIGH").otherwise("LOW").alias("stock_qty_cume_dist_tier"),
    )
    .agg(
        F.count("*").alias("event_count"),
        F.corr("price", "stock_qty").alias("corr_price_stock_qty"),
        F.bit_or("stock_qty").alias("stock_qty_bit_or"),
        F.percentile_approx("price", [0.25, 0.5, 0.75]).alias("price_quartiles"),
        F.round(F.avg("stock_qty"), 2).alias("avg_stock_qty"),
    )
    .filter(F.col("avg_stock_qty") > 0)
)

result = (
    summary
    .select(
        "stock_qty_cume_dist_tier",
        "corr_price_stock_qty",
        "stock_qty_bit_or",
        "avg_stock_qty",
    )
)
