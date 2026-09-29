from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "customer_id").orderBy(F.col("amount").desc_nulls_last())
w2 = Window.partitionBy("customer_id")
w3 = Window.partitionBy("customer_id").orderBy("shipping_fee_int")
w4 = Window.orderBy(F.col("amount").desc_nulls_first())

valid = (
    df_orders
    .filter(F.col("store_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "status",
        "channel",
        "amount",
        F.col("shipping_fee").cast("int").alias("shipping_fee_int"),
    )
)

remaining_keys = (
    valid
    .select("status", "customer_id")
    .intersect(
        df_orders
        .filter(F.col("status") == "RETURNED")
        .select("status", "customer_id")
    )
)

kept_rows = (
    valid
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

cleaned = (
    kept_rows
    .filter(F.col("customer_id").isNotNull())
    .select(
        "*",
        F.ceil("amount").alias("amount_whole"),
        F.coalesce("channel", F.lit("NONE")).alias("channel_filled"),
    )
)

metrics = (
    cleaned
    .filter(F.col("status") == "PENDING")
    .select(
        "customer_id",
        "status",
        "channel",
        "amount",
        "shipping_fee_int",
        "amount_whole",
        "channel_filled",
        F.cume_dist().over(w).alias("amount_cume_dist"),
        F.max("amount").over(w2).alias("customer_id_max_amount"),
        F.rank().over(w3).alias("shipping_fee_int_rank"),
        F.dense_rank().over(w4).alias("amount_dense_rank"),
    )
)

filtered = (
    metrics
    .filter(F.col("customer_id").isNotNull() & (F.col("channel").eqNullSafe(F.lit("store"))))
    .select(
        "status",
        "channel",
        "amount",
        "shipping_fee_int",
        "amount_whole",
        "channel_filled",
        "amount_dense_rank",
        F.round("shipping_fee_int", 0).alias("shipping_fee_int_rounded"),
        F.trim("channel").alias("clean_channel"),
        F.coalesce("channel", F.lit("UNKNOWN")).alias("resolved_channel"),
    )
)

result = (
    filtered
    .groupBy("status")
    .pivot("channel", ["mobile", "partner"])
    .agg(F.sum("amount_whole").alias("amount"), F.count(F.lit(1)).alias("cnt"))
)
