from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("order_id").orderBy(F.col("session_id").desc())
w4 = Window.partitionBy("channel").orderBy("latest_rank", "id")

with_gaps = (
    df_payments
    .withColumn("is_new_burst", F.when(F.lag("paid_at").over(w).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessions_payments = (
    with_gaps
    .select(
        "id",
        "order_id",
        "paid_at",
        "status",
        F.sum("is_new_burst").over(w2).alias("session_id"),
    )
)

deduped = (
    sessions_payments
    .withColumn("latest_rank", F.rank().over(w3))
    .filter(F.col("latest_rank") == 1)
)

joined = (
    deduped
    .alias("s")
    .join(df_orders.alias("d"), F.col("s.order_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.amount"), F.col("d.channel"))
)

scored = (
    joined
    .filter(F.col("latest_rank") >= 1)
    .select(
        "id",
        "order_id",
        "status",
        "session_id",
        "latest_rank",
        "channel",
        F.lead("session_id").over(w4).alias("next_session_id"),
        F.lag("session_id").over(w4).alias("prev_session_id"),
        F.lag("amount").over(w4).alias("prev_amount"),
        F.row_number().over(w4).alias("event_seq"),
    )
)

joined_payments = (
    scored
    .alias("src")
    .join(df_orders.alias("o"), F.col("src.order_id") == F.col("o.id"), "inner")
    .select("src.*", F.col("o.discount"), F.col("o.amount"))
)

result = (
    joined_payments
    .select(
        "id",
        "status",
        "session_id",
        "latest_rank",
        "channel",
        "next_session_id",
        "prev_session_id",
        "prev_amount",
        "event_seq",
        F.when(F.col("status").rlike("^(FAILED|SUCCESS)$"), "CORE").when(F.col("status").rlike("^(REFUNDED)$"), "GROUP_B").otherwise("OTHER_ZONE").alias("status_zone"),
    )
    .orderBy(F.col("next_session_id").asc_nulls_last())
)
