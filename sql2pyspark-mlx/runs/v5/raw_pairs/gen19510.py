from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "status")
w2 = Window.partitionBy("store_id")
w3 = Window.partitionBy("customer_id", "status")
w4 = Window.partitionBy("customer_id").orderBy(F.col("customer_id_stddev_samp_store_id_max_discount").asc_nulls_first())
w_part = Window.partitionBy("customer_id")

valid = (
    df_orders
    .filter(F.col("order_date") >= F.to_date(F.lit("2023-09-21")))
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "discount",
        (F.col("shipping_fee") - F.col("discount")).alias("shipping_fee_minus_discount"),
        F.coalesce("channel", F.lit("NONE")).alias("channel_clean"),
    )
)

remaining_keys = (
    valid
    .select("id")
    .intersectAll(
        df_orders
        .filter(F.col("discount") < 0.15)
        .select("id")
    )
)

kept_rows = (
    valid
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

metrics = (
    kept_rows
    .select(
        "id",
        "customer_id",
        "order_date",
        "status",
        "discount",
        F.max("shipping_fee_minus_discount").over(w).alias("store_id_max_shipping_fee_minus_discount"),
        F.max("discount").over(w2).alias("store_id_max_discount"),
    )
)

scored = (
    metrics
    .select(
        "id",
        "customer_id",
        "store_id_max_discount",
        F.stddev_samp("store_id_max_discount").over(w3).alias("customer_id_stddev_samp_store_id_max_discount"),
    )
)

enriched = (
    scored
    .select(
        "customer_id",
        "store_id_max_discount",
        F.rank().over(w4).alias("customer_id_stddev_samp_store_id_max_discount_rank"),
    )
)

with_windows = (
    enriched
    .filter(F.col("customer_id").isNull())
    .withColumn("customer_id_sum_store_id_max_discount", F.sum("store_id_max_discount").over(w_part))
    .withColumn("customer_id_max_customer_id_stddev_samp_store_id_max_discount_rank", F.max("customer_id_stddev_samp_store_id_max_discount_rank").over(w_part))
)

with_windows_orders = (
    with_windows
    .select(
        "store_id_max_discount",
        "customer_id_stddev_samp_store_id_max_discount_rank",
        F.round((F.col("customer_id_max_customer_id_stddev_samp_store_id_max_discount_rank") / F.sum("customer_id_max_customer_id_stddev_samp_store_id_max_discount_rank").over(w_part)), 4).alias("customer_id_max_customer_id_stddev_samp_store_id_max_discount_rank_share"),
    )
)

result = (
    with_windows_orders
    .select(
        "store_id_max_discount",
        "customer_id_stddev_samp_store_id_max_discount_rank",
        "customer_id_max_customer_id_stddev_samp_store_id_max_discount_rank_share",
    )
    .orderBy(F.col("store_id_max_discount").desc())
)
