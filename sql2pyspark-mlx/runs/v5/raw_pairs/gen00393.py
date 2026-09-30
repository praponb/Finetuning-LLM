from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("order_id").orderBy("paid_at", "id")

filtered = (
    df_payments
    .filter(F.col("paid_at") > F.to_date(F.lit("2023-09-02")))
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

enriched = (
    filtered
    .filter(F.col("method") != "bank_transfer")
    .select(
        "*",
        F.lag("amount").over(w_running).alias("prev_amount"),
        F.row_number().over(w_running).alias("event_seq"),
    )
)

with_dims = (
    enriched
    .alias("s")
    .join(df_orders.alias("p"), F.col("s.order_id") == F.col("p.id"), "inner")
    .select(
        "s.id",
        "s.order_id",
        "s.amount",
        "s.status",
        "s.method_status_key",
        "s.event_seq",
        F.col("p.channel"),
    )
)

cleaned = (
    with_dims
    .filter(F.col("amount") < 1030)
    .select(
        "*",
        F.when(F.col("status").isin(["SUCCESS", "REFUNDED"]), "Core").otherwise("Other").alias("status_group"),
        F.coalesce("channel", F.lit("N/A")).alias("channel_clean"),
    )
)

joined = (
    cleaned
    .alias("s")
    .join(df_orders.alias("p"), F.col("s.order_id") == F.col("p.id"), "left")
    .select(
        "s.*",
        F.col("p.status").alias("order_status"),
        F.col("p.discount"),
        F.col("p.channel").alias("order_channel"),
    )
)

result = (
    joined
    .select(
        "id",
        "order_id",
        "amount",
        "method_status_key",
        "event_seq",
        "channel",
        "status_group",
        "channel_clean",
        "order_status",
    )
    .orderBy(F.col("event_seq").desc_nulls_last(), F.col("order_id").desc())
)
