from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")
w2 = Window.partitionBy("merchant").orderBy(F.col("txn_time").desc(), "id")
w3 = Window.partitionBy("account_id")
w4 = Window.orderBy("amount", "id")
w5 = Window.partitionBy("merchant").orderBy("amount", "id").rowsBetween(-5, -1)
w6 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("account_id").orderBy("amount_avg_5_prior", "id")
w8 = Window.partitionBy("txn_type")
w_ordered = Window.partitionBy("txn_type").orderBy(F.col("txn_time").desc(), "id")
w10 = Window.orderBy(F.col("prev_seq_no_2_share").asc_nulls_first())
w11 = Window.partitionBy("txn_type").orderBy("prev_seq_no_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w12 = Window.partitionBy("txn_type").orderBy(F.col("prev_amount").desc_nulls_last())
w13 = Window.partitionBy("txn_type").orderBy("seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w14 = Window.orderBy("prev_seq_no_2", "id")
w15 = Window.partitionBy("txn_type").orderBy("seq_no", "id")
w16 = Window.orderBy("prev_seq_no_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scored = (
    df_transactions
    .filter(F.col("is_flagged"))
    .select(
        "*",
        F.row_number().over(w).alias("seq_no"),
        F.row_number().over(w2).alias("row_seq"),
        F.sum("amount").over(w3).alias("account_id_sum_amount"),
        F.row_number().over(w4).alias("seq_no_2"),
        F.avg("amount").over(w5).alias("amount_avg_5_prior"),
    )
)

with_gaps = (
    scored
    .withColumn("is_new_session", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

islands = with_gaps.withColumn("burst_id", F.sum("is_new_session").over(w6))

metrics = (
    islands
    .select(
        "id",
        "txn_type",
        "txn_time",
        "merchant",
        "seq_no",
        F.lag("amount").over(w7).alias("prev_amount"),
        F.lead("amount").over(w7).alias("next_amount"),
        F.lead("seq_no_2").over(w7).alias("next_seq_no_2"),
        F.lag("seq_no_2").over(w7).alias("prev_seq_no_2"),
        F.row_number().over(w7).alias("row_seq_2"),
    )
)

enriched = (
    metrics
    .select(
        "id",
        "txn_type",
        "seq_no",
        "prev_amount",
        "prev_seq_no_2",
        F.round((F.col("prev_seq_no_2") / F.sum("prev_seq_no_2").over(w8)), 4).alias("prev_seq_no_2_share"),
        F.row_number().over(w_ordered).alias("row_seq"),
    )
)

metrics_transactions = (
    enriched
    .select(
        "*",
        F.rank().over(w10).alias("prev_seq_no_2_share_rank"),
        F.first("row_seq", ignorenulls=True).over(w11).alias("first_known_row_seq"),
        F.rank().over(w12).alias("prev_amount_rank"),
    )
)

result = (
    metrics_transactions
    .select(
        "id",
        "txn_type",
        "prev_amount",
        "prev_seq_no_2_share",
        "row_seq",
        "prev_seq_no_2_share_rank",
        "first_known_row_seq",
        "prev_amount_rank",
        F.sum("prev_seq_no_2_share").over(w13).alias("running_prev_seq_no_2_share"),
        F.ntile(3).over(w14).alias("prev_seq_no_2_bucket"),
        F.lead("row_seq", 1, 0).over(w15).alias("next_row_seq"),
        F.sum("prev_seq_no_2_share").over(w16).alias("running_prev_seq_no_2_share_2"),
    )
)
