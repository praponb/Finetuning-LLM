from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_status").orderBy("paid_at", "id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status", "order_id").orderBy("order_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("status")
w5 = Window.partitionBy("order_id").orderBy(F.col("order_amount").desc_nulls_last())
w6 = Window.partitionBy("status").orderBy("next_order_amount", "id").rowsBetween(-3, -1)
amount_by_order_id = df_payments.groupBy("order_id").agg(F.max("amount").alias("max_payment_amount"))

with_dims = (
    df_payments
    .alias("src")
    .join(df_orders.alias("p"), F.col("src.order_id") == F.col("p.id"), "left")
    .select(
        "src.id",
        "src.order_id",
        "src.method",
        "src.amount",
        "src.paid_at",
        "src.status",
        F.col("p.status").alias("order_status"),
        F.col("p.amount").alias("order_amount"),
    )
)

scored = (
    with_dims
    .select(
        "id",
        "order_id",
        "status",
        "order_amount",
        F.lead("order_amount", 1).over(w).alias("next_order_amount"),
        F.last("order_status", ignorenulls=True).over(w2).alias("last_known_order_status"),
    )
)

enriched = (
    scored
    .select(
        "id",
        "order_id",
        "next_order_amount",
        "last_known_order_status",
        F.last("next_order_amount", ignorenulls=True).over(w3).alias("last_known_next_order_amount"),
        F.sum("next_order_amount").over(w4).alias("status_sum_next_order_amount"),
        F.dense_rank().over(w5).alias("order_amount_dense_rank"),
        F.avg("order_amount").over(w6).alias("order_amount_avg_3_prior"),
    )
)

joined = (
    enriched
    .alias("src")
    .join(df_orders.alias("o"), F.col("src.order_id").eqNullSafe(F.col("o.id")), "left")
    .select(
        "src.id",
        "src.order_id",
        "src.last_known_order_status",
        "src.last_known_next_order_amount",
        "src.order_amount_dense_rank",
        "src.order_amount_avg_3_prior",
        F.col("o.status"),
    )
)

base = (
    joined
    .filter(F.col("status") == "PENDING")
    .select("*", F.length("status").alias("status_length"))
)

with_dims_payments = (
    base
    .alias("e")
    .join(df_orders.alias("b"), F.col("e.order_id") == F.col("b.id"), "left")
    .select(
        "e.id",
        "e.order_id",
        "e.last_known_order_status",
        "e.status",
        "e.status_length",
        F.col("b.discount"),
        F.col("b.channel"),
        F.col("b.status").alias("order_status"),
    )
)

result = (
    with_dims_payments
    .join(amount_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "last_known_order_status",
        "status",
        "status_length",
        "channel",
        "order_status",
        F.when(F.col("channel").rlike("^(store|web)$"), "PRIORITY").when(F.col("channel").rlike("^(mobile|partner)$"), "OTHER_ZONE").otherwise("GROUP_A").alias("channel_zone"),
        F.col("max_payment_amount"),
    )
    .orderBy(F.col("channel").desc_nulls_first())
)
