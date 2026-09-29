from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("paid_at").desc(), "id")
w2 = Window.orderBy(F.col("paid_at").desc(), "id")
w3 = Window.orderBy("paid_at", "id")
w4 = Window.partitionBy("channel").orderBy(F.col("row_seq_2").desc_nulls_last(), "id")
w_recent = Window.partitionBy("channel", "order_id").orderBy("order_date", "id")
w6 = Window.orderBy("rn", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("order_id").orderBy(F.col("row_seq").desc_nulls_last())
w8 = Window.orderBy("running_row_seq_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_payments
    .filter(F.col("order_id").isNotNull())
    .select(
        "id",
        "order_id",
        "paid_at",
        "status",
        F.concat_ws("-", "status", "method").alias("status_method_key"),
        F.concat_ws("-", "status", "method").alias("status_method_key_2"),
        F.coalesce("method", F.lit("UNKNOWN")).alias("resolved_method"),
    )
)

metrics = (
    base
    .withColumn("event_seq", F.row_number().over(w))
    .withColumn("seq_no", F.row_number().over(w2))
    .withColumn("seq_no_2", F.row_number().over(w3))
    .withColumn("row_seq", F.row_number().over(w2))
    .withColumn("row_seq_2", F.row_number().over(w))
)

joined = (
    metrics
    .alias("a")
    .join(df_orders.alias("b"), F.col("a.order_id") == F.col("b.id"), "left")
    .select(
        "a.id",
        "a.order_id",
        "a.status_method_key",
        "a.status_method_key_2",
        "a.seq_no_2",
        "a.row_seq",
        "a.row_seq_2",
        F.col("b.channel"),
        F.col("b.order_date"),
        F.col("b.discount"),
    )
)

deduped = (
    joined
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
)

scored = (
    deduped
    .select(
        "id",
        "order_id",
        "status_method_key_2",
        "row_seq",
        "row_seq_2",
        "channel",
        "order_date",
        "discount",
        F.lag("row_seq_2").over(w_recent).alias("prev_row_seq_2"),
        F.lead("row_seq_2").over(w_recent).alias("next_row_seq_2"),
        F.lag("discount").over(w_recent).alias("prev_discount"),
        F.row_number().over(w_recent).alias("seq_no"),
        F.sum("row_seq_2").over(w6).alias("running_row_seq_2"),
    )
)

result = (
    scored
    .filter(F.col("row_seq") <= 7)
    .select(
        "id",
        "status_method_key_2",
        "seq_no",
        "running_row_seq_2",
        F.rank().over(w7).alias("row_seq_rank"),
        F.first("seq_no", ignorenulls=True).over(w8).alias("first_known_seq_no"),
    )
)
