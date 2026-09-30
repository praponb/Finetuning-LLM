from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy(F.col("order_amount").asc_nulls_last())
w2 = Window.partitionBy("order_id", "method").orderBy("order_amount", "id")
w_ordered = Window.partitionBy("order_id").orderBy("order_amount_dense_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("channel").orderBy("order_amount_dense_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("order_id").orderBy("paid_at", "id")
w6 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_payments
    .filter(F.col("method").eqNullSafe(F.lit("card")))
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "status",
        F.concat_ws(",", "method", "status").alias("method_status_key"),
    )
)

with_dims = (
    cleaned
    .alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .select(
        "s.*",
        F.col("o.amount").alias("order_amount"),
        F.col("o.channel"),
        F.col("o.shipping_fee"),
    )
)

filtered = (
    with_dims
    .filter(F.col("method").isNotNull())
    .select(
        "id",
        "order_id",
        "method",
        "paid_at",
        "status",
        "order_amount",
        "channel",
        "shipping_fee",
        F.concat("channel", F.lit("/"), "status").alias("channel_status"),
    )
)

scored = (
    filtered
    .filter(F.col("method").isNotNull())
    .select(
        "id",
        "order_id",
        "paid_at",
        "status",
        "channel",
        "shipping_fee",
        F.dense_rank().over(w).alias("order_amount_dense_rank"),
        F.lead("order_amount", 1).over(w2).alias("next_order_amount"),
    )
)

metrics = (
    scored
    .select(
        "id",
        "order_id",
        "paid_at",
        "shipping_fee",
        "order_amount_dense_rank",
        "next_order_amount",
        F.sum("order_amount_dense_rank").over(w_ordered).alias("running_order_amount_dense_rank"),
        F.sum("order_amount_dense_rank").over(w4).alias("running_order_amount_dense_rank_2"),
    )
)

flagged = (
    metrics
    .withColumn("session_start_flag", F.when(F.lag("paid_at").over(w5).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w5) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessionized = (
    flagged
    .select(
        "id",
        "order_id",
        "paid_at",
        "shipping_fee",
        "next_order_amount",
        "running_order_amount_dense_rank",
        F.sum("session_start_flag").over(w6).alias("burst_id"),
    )
)

result = (
    sessionized
    .withColumn("burst_id_tier", F.when(F.col("burst_id") >= 7, "PREMIUM").otherwise("STANDARD"))
    .withColumn("next_order_amount_tier", F.when(F.col("next_order_amount") >= 1240, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["burst_id_tier", "next_order_amount_tier"], ["burst_id_tier"]],
        "burst_id_tier",
        "next_order_amount_tier",
    )
    .agg(
        ((F.unix_timestamp(F.max("paid_at")) - F.unix_timestamp(F.min("paid_at"))) / 60).alias("duration_min"),
        F.percentile_approx("shipping_fee", 0.95).alias("shipping_fee_p95"),
        F.max("running_order_amount_dense_rank").alias("max_running_order_amount_dense_rank"),
    )
    .orderBy(F.col("max_running_order_amount_dense_rank").desc_nulls_first())
)
