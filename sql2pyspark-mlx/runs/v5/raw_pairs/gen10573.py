from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand")
w2 = Window.partitionBy("brand", "category")
w3 = Window.orderBy(F.col("cost").desc_nulls_first())
w4 = Window.partitionBy("supplier_id")
w_part = Window.orderBy(F.col("cost_share").asc_nulls_first())
w6 = Window.orderBy(F.col("cost_dense_rank").desc())
w_grp = Window.partitionBy("category", "supplier_id")
w8 = Window.orderBy("cost_dense_rank")

kept_keys = (
    df_products
    .select("id", "supplier_id")
    .subtract(
        df_products
        .filter(F.array_contains("tags", "eco"))
        .select("id", "supplier_id")
    )
)

kept_rows = (
    df_products
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.supplier_id") == F.col("k.supplier_id")), "inner")
    .select("s.*")
)

with_windows = (
    kept_rows
    .filter(F.col("stock_qty") <= 373)
    .select(
        "id",
        "category",
        "price",
        "cost",
        "launch_date",
        "supplier_id",
        F.round((F.col("cost") / F.sum("cost").over(w)), 4).alias("cost_share"),
        F.corr("cost", "stock_qty").over(w2).alias("corr_cost_stock_qty"),
        F.dense_rank().over(w3).alias("cost_dense_rank"),
        F.stddev_samp("price").over(w).alias("brand_stddev_samp_price"),
        F.max("stock_qty").over(w4).alias("supplier_id_max_stock_qty"),
    )
)

metrics = (
    with_windows
    .select(
        "*",
        F.cume_dist().over(w_part).alias("cost_share_cume_dist"),
        F.dense_rank().over(w6).alias("cost_dense_rank_dense_rank"),
        F.stddev_samp("brand_stddev_samp_price").over(w_grp).alias("category_stddev_samp_brand_stddev_samp_price"),
        F.dense_rank().over(w8).alias("cost_dense_rank_dense_rank_2"),
    )
)

remaining_keys = (
    metrics
    .select("supplier_id", "id")
    .intersect(
        df_products
        .filter(F.col("cost") < 210)
        .select("supplier_id", "id")
    )
)

kept_rows_products = (
    metrics
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.supplier_id") == F.col("k.supplier_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

joined = (
    kept_rows_products
    .alias("s")
    .join(df_suppliers.alias("d"), F.col("s.supplier_id") == F.col("d.id"), "inner")
    .select("s.*", F.col("d.country"), F.col("d.rating"))
)

result = (
    joined
    .select(
        "id",
        "category",
        "price",
        "cost",
        "supplier_id",
        "brand_stddev_samp_price",
        "cost_share_cume_dist",
        "category_stddev_samp_brand_stddev_samp_price",
        "cost_dense_rank_dense_rank_2",
    )
)
