from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("method").orderBy("paid_at", "id")
w_running = Window.orderBy(F.col("paid_at").desc(), "id")
w_hist_2 = Window.orderBy("paid_at", "id")
w4 = Window.orderBy(F.col("seq_no").desc(), "id")
w5 = Window.partitionBy("method").orderBy("seq_no", "id")
w6 = Window.orderBy(F.col("seq_no").desc())
w7 = Window.partitionBy("method").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-2592000, Window.currentRow)

cleaned = (
    df_payments
    .filter(~F.col("method").eqNullSafe(F.lit("promptpay")))
    .select(
        "id",
        "method",
        "paid_at",
        F.coalesce("method", F.lit("unassigned")).alias("method_filled"),
        F.concat_ws("-", "status", "method").alias("status_method_key"),
        F.coalesce("method", F.lit("OTHER")).alias("method_filled_2"),
    )
)

with_windows = (
    cleaned
    .select(
        "id",
        "method",
        "paid_at",
        "method_filled",
        "method_filled_2",
        F.row_number().over(w_hist).alias("seq_no"),
        F.row_number().over(w_running).alias("event_seq"),
        F.row_number().over(w_hist_2).alias("seq_no_2"),
    )
)

with_windows_payments = (
    with_windows
    .select(
        "id",
        "method",
        "paid_at",
        "method_filled",
        F.ntile(10).over(w4).alias("seq_no_bucket"),
        F.lead("event_seq", 2).over(w5).alias("next_event_seq"),
        F.cume_dist().over(w6).alias("seq_no_cume_dist"),
        F.max("seq_no").over(w7).alias("seq_no_max_30d"),
    )
)

scored = (
    with_windows_payments
    .filter(F.col("seq_no_cume_dist") < 0.23)
    .select(
        "id",
        "method",
        "seq_no_cume_dist",
        "seq_no_max_30d",
        F.lag("seq_no_bucket").over(w_hist).alias("prev_seq_no_bucket"),
        F.lead("next_event_seq").over(w_hist).alias("next_next_event_seq"),
        F.row_number().over(w_hist).alias("event_seq"),
    )
)

cleaned_payments = (
    scored
    .filter((F.col("method").eqNullSafe(F.lit("paypal"))) & (F.col("method") == "paypal"))
    .select(
        "*",
        (F.col("next_next_event_seq") * F.col("event_seq")).alias("next_next_event_seq_times_event_seq"),
        (F.col("seq_no_cume_dist") - F.col("next_next_event_seq")).alias("seq_no_cume_dist_minus_next_next_event_seq"),
    )
)

base = (
    cleaned_payments
    .filter(F.col("method").isNotNull())
    .select(
        "id",
        "seq_no_cume_dist",
        "seq_no_max_30d",
        "next_next_event_seq",
        "event_seq",
        "next_next_event_seq_times_event_seq",
        "seq_no_cume_dist_minus_next_next_event_seq",
        F.when(F.col("prev_seq_no_bucket") >= 8, "High").when(F.col("prev_seq_no_bucket") >= 2, "Medium").otherwise("Low").alias("prev_seq_no_bucket_tier"),
        F.coalesce("method", F.lit("UNKNOWN")).alias("resolved_method"),
    )
)

report = (
    base
    .select(
        "id",
        "seq_no_max_30d",
        "next_next_event_seq",
        "event_seq",
        "next_next_event_seq_times_event_seq",
        "seq_no_cume_dist_minus_next_next_event_seq",
        "resolved_method",
    )
)

result = (
    report
    .select(
        "id",
        "seq_no_max_30d",
        "next_next_event_seq_times_event_seq",
        "seq_no_cume_dist_minus_next_next_event_seq",
        "resolved_method",
    )
    .orderBy(
        F.col("next_next_event_seq_times_event_seq").asc_nulls_first(),
        "seq_no_max_30d",
    )
)
