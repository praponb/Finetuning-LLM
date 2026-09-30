from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("paid_at_period").desc(), "id")
w_hist = Window.partitionBy("order_id").orderBy("paid_at_period", "id")
w_recent = Window.partitionBy("order_id").orderBy(F.col("paid_at_period").desc(), "id")
w_hist_2 = Window.orderBy("paid_at_period", "id")
w5 = Window.orderBy("seq_no_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("order_id")
w7 = Window.partitionBy("order_id").orderBy("event_seq_2", "id")
amount_by_order_id = df_payments.groupBy("order_id").agg(F.max("amount").alias("max_payment_amount"))

base = (
    df_payments
    .filter((F.col("paid_at") > F.lit("2023-09-01 04:00:00").cast("timestamp")) & F.col("order_id").isNotNull() & F.col("status").isin(["SUCCESS", "FAILED"]))
    .select(
        "id",
        "order_id",
        F.date_trunc("month", "paid_at").alias("paid_at_period"),
        F.coalesce("method", F.lit("N/A")).alias("resolved_method"),
    )
)

scored = (
    base
    .select(
        "*",
        F.row_number().over(w).alias("event_seq"),
        F.row_number().over(w_hist).alias("row_seq"),
        F.row_number().over(w_hist).alias("event_seq_2"),
        F.row_number().over(w_recent).alias("seq_no"),
        F.row_number().over(w_hist_2).alias("seq_no_2"),
    )
)

scored_payments = (
    scored
    .select(
        "*",
        F.sum("seq_no_2").over(w5).alias("cumulative_seq_no_2"),
        F.round((F.col("seq_no") / F.sum("seq_no").over(w6)), 4).alias("seq_no_share"),
        F.ntile(3).over(w7).alias("event_seq_2_bucket"),
    )
)

result = (
    scored_payments
    .join(amount_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "paid_at_period",
        "resolved_method",
        "row_seq",
        "event_seq_2",
        "seq_no",
        "seq_no_2",
        "cumulative_seq_no_2",
        F.col("max_payment_amount"),
    )
    .orderBy(F.col("paid_at_period").desc())
)
