from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w2 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("txn_type")
w4 = Window.partitionBy("txn_type", "account_id").orderBy(F.col("txn_time").desc(), "id")
w5 = Window.partitionBy("status").orderBy("txn_time", "id")
w6 = Window.partitionBy("txn_type").orderBy(F.col("event_seq").desc(), "id")
w_part = Window.partitionBy("txn_type").orderBy("event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("txn_type").orderBy("next_amount_adjusted_2", "id").rowsBetween(-6, Window.currentRow)

base = (
    df_transactions
    .filter(F.col("txn_time") >= F.lit("2023-09-27").cast("date"))
    .withColumn("amount_adjusted", F.round((F.col("amount") * 1.07), 2))
    .withColumn("merchant_txn_type_key", F.concat_ws(",", "merchant", "txn_type"))
)

enriched = (
    base
    .alias("s")
    .join(df_accounts.alias("b"), F.col("s.account_id") == F.col("b.id"), "inner")
    .filter(F.col("b.opened_date") <= "2025-05-01")
    .select(
        "s.id",
        "s.account_id",
        "s.txn_type",
        "s.amount",
        "s.txn_time",
        "s.is_flagged",
        "s.amount_adjusted",
        F.col("b.status"),
    )
)

gaps = (
    enriched
    .withColumn("is_new_burst", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

sessionized = (
    gaps
    .select(
        "id",
        "account_id",
        "txn_type",
        "txn_time",
        "amount_adjusted",
        "status",
        F.sum("is_new_burst").over(w2).alias("burst_id"),
    )
)

enriched_transactions = (
    sessionized
    .select(
        "id",
        "txn_type",
        "txn_time",
        "amount_adjusted",
        F.max("burst_id").over(w3).alias("txn_type_max_burst_id"),
        F.row_number().over(w4).alias("event_seq"),
        F.lead("amount_adjusted", 1).over(w5).alias("next_amount_adjusted"),
    )
)

ranked = (
    enriched_transactions
    .withColumn("latest_rank", F.row_number().over(w6))
    .filter(F.col("latest_rank") == 1)
)

with_windows = (
    ranked
    .withColumn("next_amount_adjusted_2", F.lead("amount_adjusted").over(w))
    .withColumn("prev_amount_adjusted", F.lag("amount_adjusted").over(w))
    .withColumn("prev_txn_type_max_burst_id", F.lag("txn_type_max_burst_id").over(w))
    .withColumn("next_txn_type_max_burst_id", F.lead("txn_type_max_burst_id").over(w))
    .withColumn("seq_no", F.row_number().over(w))
)

enriched_transactions_2 = (
    with_windows
    .withColumn("running_count_prev_amount_adjusted", F.count("prev_amount_adjusted").over(w_part))
    .withColumn("running_min_prev_amount_adjusted", F.min("prev_amount_adjusted").over(w_part))
    .withColumn("running_sum_prev_amount_adjusted", F.sum("prev_amount_adjusted").over(w_part))
    .withColumn("next_amount_adjusted_2_avg_6", F.avg("next_amount_adjusted_2").over(w8))
)

result = (
    enriched_transactions_2
    .select(
        "id",
        "txn_type",
        "txn_time",
        "txn_type_max_burst_id",
        "next_amount_adjusted",
        "next_amount_adjusted_2",
        "prev_amount_adjusted",
        "prev_txn_type_max_burst_id",
        "seq_no",
    )
    .orderBy(F.col("seq_no").desc_nulls_first())
)
