from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("store_id", "product_id").orderBy(F.col("stock_qty").asc_nulls_last())
w2 = Window.orderBy(F.col("units").desc_nulls_last())
w3 = Window.orderBy(F.col("stock_qty_dense_rank").desc_nulls_last())
w4 = Window.orderBy(F.col("stock_qty_dense_rank").desc())
w5 = Window.orderBy(F.col("units_dense_rank").asc_nulls_last())
w6 = Window.orderBy(F.col("stock_qty_dense_rank_percent_rank").desc_nulls_last())

enriched = (
    df_store_sales
    .alias("src")
    .join(df_products.alias("p"), F.col("src.product_id") == F.col("p.id"), "left")
    .select("src.*", F.col("p.cost"), F.col("p.stock_qty"), F.col("p.launch_date"))
)

remaining_keys = (
    enriched
    .select("product_id", "store_id")
    .intersectAll(
        df_store_sales
        .filter(F.col("product_id").isin([2, 9]))
        .select("product_id", "store_id")
    )
)

kept_rows = (
    enriched
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")) & (F.col("s.store_id") == F.col("k.store_id")), "inner")
    .select("s.*")
)

metrics = (
    kept_rows
    .select(
        "id",
        "units",
        "revenue",
        "stock_qty",
        "launch_date",
        F.dense_rank().over(w_recent).alias("stock_qty_dense_rank"),
    )
)

scored = (
    metrics
    .select(
        "id",
        "launch_date",
        "stock_qty_dense_rank",
        F.dense_rank().over(w2).alias("units_dense_rank"),
        F.percent_rank().over(w3).alias("stock_qty_dense_rank_percent_rank"),
        F.dense_rank().over(w4).alias("stock_qty_dense_rank_dense_rank"),
    )
)

enriched_store_sales = (
    scored
    .filter(F.col("stock_qty_dense_rank") <= 5)
    .select(
        "stock_qty_dense_rank",
        "stock_qty_dense_rank_percent_rank",
        "stock_qty_dense_rank_dense_rank",
        F.rank().over(w5).alias("units_dense_rank_rank"),
        F.dense_rank().over(w6).alias("stock_qty_dense_rank_percent_rank_dense_rank"),
        F.cume_dist().over(w3).alias("stock_qty_dense_rank_cume_dist"),
    )
)

result = (
    enriched_store_sales
    .select(
        "stock_qty_dense_rank",
        "stock_qty_dense_rank_dense_rank",
        "units_dense_rank_rank",
        "stock_qty_dense_rank_percent_rank_dense_rank",
        "stock_qty_dense_rank_cume_dist",
    )
    .orderBy(F.col("stock_qty_dense_rank_dense_rank").asc_nulls_first())
)
