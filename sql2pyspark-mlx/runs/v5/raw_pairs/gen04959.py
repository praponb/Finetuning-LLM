from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "method").orderBy("is_refunded", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.orderBy("is_refunded", "id")
w3 = Window.partitionBy("status").orderBy(F.col("amount").desc())
w4 = Window.partitionBy("status").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("status").orderBy("cumulative_is_refunded", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("method").orderBy("prev_is_refunded", "id")
w7 = Window.partitionBy("status").orderBy("is_refunded_avg_6", "id").rowsBetween(-6, -1)
w8 = Window.orderBy("cumulative_is_refunded_avg_6_prior", "id")
w_recent = Window.partitionBy("status").orderBy(F.col("is_refunded").asc_nulls_first())
w10 = Window.orderBy("seq_no", "id")
w11 = Window.partitionBy("status")
w12 = Window.orderBy("prev_is_refunded_2")
w13 = Window.partitionBy("status").orderBy("is_refunded", "id").rowsBetween(-5, -1)
w14 = Window.partitionBy("method").orderBy("cumulative_amount_cume_dist", "id")
w15 = Window.partitionBy("method").orderBy("prev_is_refunded", "id").rowsBetween(-5, -1)

cleaned = (
    df_payments
    .filter((F.col("paid_at") > F.to_date(F.lit("2023-05-27"))) & (F.col("status") == "SUCCESS"))
    .select(
        "id",
        "method",
        "amount",
        "status",
        F.lower("method").alias("method_lower"),
        F.when(F.col("status") == "REFUNDED", 1).otherwise(0).alias("is_refunded"),
        F.coalesce("method", F.lit("UNKNOWN")).alias("method_filled"),
    )
)

with_windows = (
    cleaned
    .filter(F.col("amount") < 850)
    .select(
        "*",
        F.avg("is_refunded").over(w).alias("is_refunded_avg_6"),
        F.lag("is_refunded", 2).over(w2).alias("prev_is_refunded"),
        F.cume_dist().over(w3).alias("amount_cume_dist"),
        F.sum("is_refunded").over(w4).alias("cumulative_is_refunded"),
    )
)

enriched = (
    with_windows
    .select(
        "id",
        "method",
        "status",
        "is_refunded",
        "method_filled",
        F.sum("amount_cume_dist").over(w_grp).alias("cumulative_amount_cume_dist"),
        F.row_number().over(w6).alias("seq_no"),
        F.avg("cumulative_is_refunded").over(w7).alias("cumulative_is_refunded_avg_6_prior"),
    )
)

with_windows_payments = (
    enriched
    .withColumn("prev_is_refunded", F.lag("is_refunded", 1).over(w8))
    .withColumn("is_refunded_dense_rank", F.dense_rank().over(w_recent))
    .withColumn("prev_is_refunded_2", F.lag("is_refunded", 1).over(w10))
)

with_windows_payments_2 = (
    with_windows_payments
    .select(
        "id",
        "method",
        "status",
        "is_refunded",
        "seq_no",
        "cumulative_is_refunded_avg_6_prior",
        "prev_is_refunded_2",
        F.avg("is_refunded").over(w11).alias("status_avg_is_refunded"),
        F.percent_rank().over(w12).alias("prev_is_refunded_2_percent_rank"),
        F.avg("prev_is_refunded_2").over(w13).alias("prev_is_refunded_2_avg_5_prior"),
        F.row_number().over(w14).alias("row_seq"),
        F.avg("cumulative_is_refunded_avg_6_prior").over(w15).alias("cumulative_is_refunded_avg_6_prior_avg_5_prior"),
    )
)

result = (
    with_windows_payments_2
    .select(
        "id",
        "method",
        "is_refunded",
        "seq_no",
        "prev_is_refunded_2_avg_5_prior",
        "row_seq",
        "cumulative_is_refunded_avg_6_prior_avg_5_prior",
    )
    .orderBy("seq_no")
)
