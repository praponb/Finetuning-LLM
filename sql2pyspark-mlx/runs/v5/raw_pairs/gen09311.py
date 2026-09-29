from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("product_id", "store_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("product_id").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("product_id").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("store_id").orderBy("launch_date", "id").rowsBetween(-2, -1)
w5 = Window.partitionBy("store_id").orderBy(F.col("cost_avg_2_prior").desc_nulls_last())
w6 = Window.partitionBy("store_id").orderBy("cost_avg_2_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("product_id")
w8 = Window.orderBy("running_cost_avg_2_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.orderBy(F.col("product_id_stddev_samp_cost_avg_2_prior_dense_rank").asc_nulls_first())
w10 = Window.partitionBy("store_id", "region")
w11 = Window.orderBy(F.col("cost_avg_2_prior").desc_nulls_last())

with_windows = (
    df_inventory
    .select(
        "id",
        "store_id",
        "product_id",
        "last_restock",
        F.min("quantity").over(w_grp).alias("running_min_quantity"),
        F.sum("quantity").over(w_grp).alias("running_sum_quantity"),
        F.count("quantity").over(w_grp).alias("running_count_quantity"),
        F.sum("quantity").over(w_recent).alias("running_quantity"),
    )
)

with_dims = (
    with_windows
    .alias("a")
    .join(df_products.alias("d"), F.col("a.product_id") == F.col("d.id"), "left")
    .select(
        "a.id",
        "a.store_id",
        "a.product_id",
        "a.running_sum_quantity",
        F.col("d.launch_date"),
        F.col("d.price"),
        F.col("d.cost"),
    )
)

enriched = (
    with_dims
    .select(
        "*",
        F.last("cost", ignorenulls=True).over(w3).alias("last_known_cost"),
        F.avg("cost").over(w4).alias("cost_avg_2_prior"),
    )
)

enriched_inventory = (
    enriched
    .filter(F.col("price") >= 350)
    .withColumn("cost_avg_2_prior_dense_rank", F.dense_rank().over(w5))
    .withColumn("running_cost_avg_2_prior", F.sum("cost_avg_2_prior").over(w6))
)

with_dims_inventory = (
    enriched_inventory
    .alias("s")
    .join(df_stores.alias("b"), F.col("s.store_id") == F.col("b.id"), "left")
    .select("s.*", F.col("b.region"))
)

metrics = (
    with_dims_inventory
    .select(
        "*",
        F.stddev_samp("cost_avg_2_prior_dense_rank").over(w_running).alias("product_id_stddev_samp_cost_avg_2_prior_dense_rank"),
        F.sum("price").over(w8).alias("running_price"),
    )
)

remaining_keys = (
    metrics
    .select("product_id", "id")
    .exceptAll(
        df_inventory
        .filter(F.month("last_restock").isin([2, 3, 6]))
        .select("product_id", "id")
    )
)

kept_rows = (
    metrics
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select(
        "id",
        "store_id",
        "product_id",
        "running_sum_quantity",
        "launch_date",
        "cost",
        "last_known_cost",
        "cost_avg_2_prior_dense_rank",
        "running_cost_avg_2_prior",
        "region",
        "product_id_stddev_samp_cost_avg_2_prior_dense_rank",
        "running_price",
        F.cume_dist().over(w9).alias("product_id_stddev_samp_cost_avg_2_prior_dense_rank_cume_dist"),
        F.round((F.col("cost_avg_2_prior_dense_rank") / F.sum("cost_avg_2_prior_dense_rank").over(w10)), 4).alias("cost_avg_2_prior_dense_rank_share"),
        F.percent_rank().over(w11).alias("cost_avg_2_prior_percent_rank"),
    )
)
