from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("amount", "id")
w2 = Window.partitionBy("status").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("order_id").orderBy(F.col("amount").desc(), "id")
w4 = Window.orderBy("amount", "id")
w5 = Window.partitionBy("order_id", "method").orderBy("amount", "id").rowsBetween(-3, Window.currentRow)
w6 = Window.partitionBy("order_id", "status").orderBy("last4_max_first_known_amount", "id")
w7 = Window.partitionBy("order_id", "status")
w8 = Window.orderBy("last4_avg_first_known_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.orderBy("seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.orderBy("last4_sum_first_known_amount", "id")
w11 = Window.orderBy(F.col("last4_sum_first_known_amount").desc(), "id")
w12 = Window.orderBy("first_known_amount", "id").rowsBetween(-3, Window.currentRow)
w13 = Window.orderBy("last4_max_first_known_amount_bucket", "id")
w14 = Window.orderBy("amount_bucket_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w15 = Window.orderBy("cumulative_seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_payments
    .filter((F.col("paid_at") >= F.lit("2023-09-15").cast("date")) & (F.col("amount") < 490))
    .select("*", F.lower("method").alias("method_lower"))
)

scored = (
    base
    .withColumn("prev_amount", F.lag("amount", 2).over(w))
    .withColumn("first_known_amount", F.first("amount", ignorenulls=True).over(w2))
    .withColumn("amount_bucket", F.ntile(5).over(w3))
    .withColumn("seq_no", F.row_number().over(w4))
    .withColumn("amount_bucket_2", F.ntile(5).over(w3))
)

enriched = (
    scored
    .select(
        "id",
        "order_id",
        "status",
        "method_lower",
        "prev_amount",
        "first_known_amount",
        "amount_bucket",
        "seq_no",
        "amount_bucket_2",
        F.avg("first_known_amount").over(w5).alias("last4_avg_first_known_amount"),
        F.max("first_known_amount").over(w5).alias("last4_max_first_known_amount"),
        F.sum("first_known_amount").over(w5).alias("last4_sum_first_known_amount"),
    )
)

enriched_payments = (
    enriched
    .filter(F.col("prev_amount").between(960, 1550))
    .select(
        "id",
        "first_known_amount",
        "seq_no",
        "amount_bucket_2",
        "last4_avg_first_known_amount",
        "last4_max_first_known_amount",
        "last4_sum_first_known_amount",
        F.ntile(3).over(w6).alias("last4_max_first_known_amount_bucket"),
        F.count("last4_sum_first_known_amount").over(w7).alias("order_id_count_last4_sum_first_known_amount"),
    )
)

with_windows = (
    enriched_payments
    .select(
        "*",
        F.sum("seq_no").over(w8).alias("cumulative_seq_no"),
        F.sum("amount_bucket_2").over(w9).alias("running_amount_bucket_2"),
        F.ntile(4).over(w10).alias("last4_sum_first_known_amount_bucket"),
        F.ntile(10).over(w11).alias("last4_sum_first_known_amount_bucket_2"),
    )
)

metrics = (
    with_windows
    .select(
        "id",
        "first_known_amount",
        "seq_no",
        "last4_sum_first_known_amount",
        "order_id_count_last4_sum_first_known_amount",
        "cumulative_seq_no",
        "last4_sum_first_known_amount_bucket",
        "last4_sum_first_known_amount_bucket_2",
        F.avg("amount_bucket_2").over(w12).alias("amount_bucket_2_avg_3"),
        F.lag("seq_no", 1).over(w13).alias("prev_seq_no"),
        F.sum("last4_sum_first_known_amount_bucket_2").over(w14).alias("cumulative_last4_sum_first_known_amount_bucket_2"),
    )
)

metrics_payments = (
    metrics
    .select(
        "*",
        F.min("seq_no").over(w15).alias("running_min_seq_no"),
        F.count("seq_no").over(w15).alias("running_count_seq_no"),
        F.max("seq_no").over(w15).alias("running_max_seq_no"),
        F.sum("seq_no").over(w15).alias("running_sum_seq_no"),
    )
)

result = (
    metrics_payments
    .cube("cumulative_seq_no")
    .agg(
        F.max("running_count_seq_no").alias("max_running_count_seq_no"),
        F.avg("cumulative_seq_no").alias("avg_cumulative_seq_no"),
    )
    .filter(F.col("avg_cumulative_seq_no") > 0)
    .orderBy(F.col("max_running_count_seq_no").desc())
)
