from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("store_id", "product_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("store_id").orderBy("quantity", "id").rowsBetween(-3, -1)
w3 = Window.partitionBy("product_id").orderBy(F.col("running_max_quantity").desc_nulls_last(), "id")
w_hist = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(-3, Window.currentRow)
w5 = Window.partitionBy("store_id").orderBy("quantity_avg_3_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("store_id").orderBy(F.col("running_sum_last4_min_quantity_avg_3_prior").desc(), "id")
w7 = Window.partitionBy("store_id").orderBy("last4_max_quantity_avg_3_prior", "id").rowsBetween(-6, Window.currentRow)
w8 = Window.partitionBy("store_id").orderBy("last4_avg_quantity_avg_3_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("store_id").orderBy(F.col("running_max_last4_min_quantity_avg_3_prior_avg_6").desc_nulls_last())
shipping_fee_by_store_id = df_orders.groupBy("store_id").agg(F.max("shipping_fee").alias("max_order_shipping_fee"))

with_windows = (
    df_inventory
    .select(
        "*",
        F.count("quantity").over(w_grp).alias("running_count_quantity"),
        F.max("quantity").over(w_grp).alias("running_max_quantity"),
        F.min("quantity").over(w_grp).alias("running_min_quantity"),
        F.avg("quantity").over(w2).alias("quantity_avg_3_prior"),
    )
)

latest = (
    with_windows
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

enriched = (
    latest
    .select(
        "id",
        "store_id",
        "quantity",
        "running_count_quantity",
        "running_min_quantity",
        "quantity_avg_3_prior",
        F.avg("quantity_avg_3_prior").over(w_hist).alias("last4_avg_quantity_avg_3_prior"),
        F.max("quantity_avg_3_prior").over(w_hist).alias("last4_max_quantity_avg_3_prior"),
        F.min("quantity_avg_3_prior").over(w_hist).alias("last4_min_quantity_avg_3_prior"),
    )
)

with_windows_inventory = (
    enriched
    .select(
        "id",
        "store_id",
        "running_min_quantity",
        "quantity_avg_3_prior",
        "last4_avg_quantity_avg_3_prior",
        "last4_max_quantity_avg_3_prior",
        F.sum("last4_min_quantity_avg_3_prior").over(w5).alias("running_sum_last4_min_quantity_avg_3_prior"),
        F.avg("last4_min_quantity_avg_3_prior").over(w5).alias("running_avg_last4_min_quantity_avg_3_prior"),
        F.max("last4_min_quantity_avg_3_prior").over(w5).alias("running_max_last4_min_quantity_avg_3_prior"),
    )
)

with_windows_inventory_2 = (
    with_windows_inventory
    .filter(F.col("running_max_last4_min_quantity_avg_3_prior") >= 80)
    .select(
        "id",
        "store_id",
        "quantity_avg_3_prior",
        "running_avg_last4_min_quantity_avg_3_prior",
        "running_max_last4_min_quantity_avg_3_prior",
        F.ntile(5).over(w6).alias("running_sum_last4_min_quantity_avg_3_prior_bucket"),
        F.avg("running_max_last4_min_quantity_avg_3_prior").over(w7).alias("running_max_last4_min_quantity_avg_3_prior_avg_6"),
        F.sum("last4_avg_quantity_avg_3_prior").over(w8).alias("running_last4_avg_quantity_avg_3_prior"),
    )
)

deduped = (
    with_windows_inventory_2
    .withColumn("dedup_rank", F.rank().over(w9))
    .filter(F.col("dedup_rank") == 1)
)

base = (
    deduped
    .filter(F.col("running_sum_last4_min_quantity_avg_3_prior_bucket") < 6)
    .select(
        "id",
        "store_id",
        "quantity_avg_3_prior",
        "running_avg_last4_min_quantity_avg_3_prior",
        "running_max_last4_min_quantity_avg_3_prior",
        "running_last4_avg_quantity_avg_3_prior",
        "dedup_rank",
        (F.col("running_sum_last4_min_quantity_avg_3_prior_bucket") - F.col("dedup_rank")).alias("running_sum_last4_min_quantity_avg_3_prior_bucket_minus_dedup_rank"),
    )
)

result = (
    base
    .join(shipping_fee_by_store_id, on="store_id", how="left")
    .select(
        "id",
        "store_id",
        "quantity_avg_3_prior",
        "running_avg_last4_min_quantity_avg_3_prior",
        "running_last4_avg_quantity_avg_3_prior",
        "dedup_rank",
        F.col("max_order_shipping_fee"),
    )
    .orderBy(
        F.col("running_last4_avg_quantity_avg_3_prior").desc_nulls_first(),
        F.col("quantity_avg_3_prior").desc(),
    )
)
