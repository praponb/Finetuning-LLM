from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("status").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("status").orderBy("running_sum_amount_plus_running_count_amount", "id").rowsBetween(-5, -1)
w3 = Window.partitionBy("status").orderBy("running_avg_amount", "id")
w4 = Window.partitionBy("status").orderBy("running_avg_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("status").orderBy(F.col("running_avg_amount").desc(), "id")
w6 = Window.partitionBy("status").orderBy("running_sum_amount_plus_running_count_amount_avg_5_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("status").orderBy(F.col("event_seq").asc_nulls_last(), "id")

valid = (
    df_payments
    .filter(F.col("paid_at") > F.lit("2023-10-27").cast("date"))
    .select(
        "id",
        "order_id",
        "amount",
        "status",
        F.when(F.col("method").isin(["paypal", "cash"]), "Priority").otherwise("Other").alias("method_group"),
        F.coalesce("method", F.lit("unassigned")).alias("method_filled"),
    )
)

with_windows = (
    valid
    .filter(F.col("amount").between(1030, 1330))
    .select(
        "*",
        F.avg("amount").over(w_hist).alias("running_avg_amount"),
        F.count("amount").over(w_hist).alias("running_count_amount"),
        F.sum("amount").over(w_hist).alias("running_sum_amount"),
    )
)

filtered = (
    with_windows
    .filter(F.col("order_id").isNotNull())
    .select(
        "id",
        "amount",
        "status",
        "method_filled",
        "running_avg_amount",
        "running_sum_amount",
        (F.col("running_count_amount") * F.col("amount")).alias("running_count_amount_times_amount"),
        (F.col("running_sum_amount") + F.col("running_count_amount")).alias("running_sum_amount_plus_running_count_amount"),
    )
)

metrics = (
    filtered
    .select(
        "id",
        "status",
        "method_filled",
        "running_avg_amount",
        "running_sum_amount",
        F.avg("running_sum_amount_plus_running_count_amount").over(w2).alias("running_sum_amount_plus_running_count_amount_avg_5_prior"),
        F.row_number().over(w3).alias("seq_no"),
    )
)

with_windows_payments = (
    metrics
    .select(
        "id",
        "status",
        "running_avg_amount",
        "running_sum_amount_plus_running_count_amount_avg_5_prior",
        "seq_no",
        F.sum("seq_no").over(w4).alias("cumulative_seq_no"),
        F.row_number().over(w5).alias("event_seq"),
        F.sum("seq_no").over(w6).alias("running_seq_no"),
    )
)

deduped = (
    with_windows_payments
    .withColumn("latest_rank", F.row_number().over(w7))
    .filter(F.col("latest_rank") == 1)
)

result = (
    deduped
    .select(
        "id",
        "status",
        "running_avg_amount",
        "running_sum_amount_plus_running_count_amount_avg_5_prior",
        "cumulative_seq_no",
        "event_seq",
        "running_seq_no",
    )
)
