from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("status").orderBy("order_date", "id")
w2 = Window.partitionBy("customer_id").orderBy("next_discount", "id")
amount_by_customer_id = df_orders.groupBy("customer_id").agg(F.avg("amount").alias("avg_order_amount"))

cleaned = (
    df_orders
    .filter(F.col("order_date") > F.to_date(F.lit("2023-05-31")))
    .select(
        "id",
        "customer_id",
        "order_date",
        "status",
        "channel",
        "discount",
        F.upper("channel").alias("channel_upper"),
        F.coalesce("channel", F.lit("UNKNOWN")).alias("resolved_channel"),
    )
)

with_windows = (
    cleaned
    .select(
        "id",
        "customer_id",
        "order_date",
        "discount",
        F.lead("discount").over(w_running).alias("next_discount"),
        F.lag("discount").over(w_running).alias("prev_discount"),
        F.row_number().over(w_running).alias("event_seq"),
    )
)

deduped = (
    with_windows
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
)

remaining_keys = (
    deduped
    .select("id", "customer_id")
    .intersect(
        df_orders
        .filter(F.col("customer_id") == 20)
        .select("id", "customer_id")
    )
)

verified = (
    deduped
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

kept = (
    verified
    .alias("s")
    .join(df_payments.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.paid_at") <= F.col("s.order_date")), "left_anti")
)

result = (
    kept
    .join(amount_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "order_date",
        "next_discount",
        "prev_discount",
        "event_seq",
        "rank_in_group",
        F.col("avg_order_amount"),
    )
)
