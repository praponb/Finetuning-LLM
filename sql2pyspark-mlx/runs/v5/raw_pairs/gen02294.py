from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy("quantity")
w_grp = Window.partitionBy("store_id", "product_id").orderBy("quantity", "id").rowsBetween(-2, Window.currentRow)
w3 = Window.orderBy("quantity", "id")
w4 = Window.partitionBy("product_id")
w_running = Window.partitionBy("product_id").orderBy(F.col("quantity_dense_rank").desc_nulls_first())

enriched = (
    df_inventory
    .select(
        "id",
        "product_id",
        "quantity",
        "last_restock",
        F.dense_rank().over(w).alias("quantity_dense_rank"),
        F.avg("quantity").over(w_grp).alias("quantity_avg_2"),
        F.ntile(5).over(w3).alias("quantity_bucket"),
    )
)

kept_keys = (
    enriched
    .select("id", "product_id")
    .exceptAll(
        df_inventory
        .filter(F.col("store_id").isin([2, 4, 6]))
        .select("id", "product_id")
    )
)

kept_rows = (
    enriched
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

enriched_inventory = (
    kept_rows
    .alias("e")
    .join(df_products.alias("d"), F.col("e.product_id") == F.col("d.id"), "left")
    .filter(F.col("d.cost").between(320, 440))
    .select("e.*", F.col("d.stock_qty"))
)

valid = (
    enriched_inventory
    .filter((F.col("last_restock") >= F.to_date(F.lit("2023-06-08"))) & F.month("last_restock").isin([4, 11, 12]))
    .select(
        "product_id",
        "quantity",
        "last_restock",
        "quantity_dense_rank",
        "quantity_avg_2",
        "quantity_bucket",
        "stock_qty",
        F.greatest("quantity_avg_2", "stock_qty").alias("greatest_quantity_avg_2_stock_qty"),
    )
)

with_windows = (
    valid
    .select(
        "*",
        F.sum("quantity").over(w4).alias("product_id_sum_quantity"),
        F.rank().over(w_running).alias("quantity_dense_rank_rank"),
        F.dense_rank().over(w_running).alias("quantity_dense_rank_dense_rank"),
    )
)

result = (
    with_windows
    .alias("a")
    .join(df_products.alias("dim"), F.col("a.product_id") == F.col("dim.id"), "inner")
    .rollup(F.col("dim.category").alias("category"))
    .agg(
        F.count("*").alias("rows_total"),
        F.min(F.col("a.stock_qty")).alias("min_stock_qty"),
        F.min(F.col("a.quantity_bucket")).alias("min_quantity_bucket"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy(F.col("min_quantity_bucket").asc_nulls_first())
)
