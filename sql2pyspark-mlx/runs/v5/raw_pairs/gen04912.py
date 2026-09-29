from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type")
w_hist = Window.orderBy(F.col("amount").desc_nulls_last())
w3 = Window.orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("account_id").orderBy("txn_type_min_amount", "id")
w5 = Window.partitionBy("account_id", "merchant").orderBy(F.col("amount_percent_rank").desc(), "id")
w6 = Window.partitionBy("txn_type").orderBy("amount_percent_rank", "id")

base = (
    df_transactions
    .filter((F.col("txn_time") >= F.lit("2023-07-09 17:00:00").cast("timestamp")) & F.col("merchant").isNotNull())
    .withColumn("is_flagged_label", F.when(F.col("is_flagged"), "Yes").otherwise("No"))
)

with_windows = (
    base
    .filter(F.col("is_flagged"))
    .withColumn("txn_type_min_amount", F.min("amount").over(w))
    .withColumn("amount_percent_rank", F.percent_rank().over(w_hist))
    .withColumn("running_amount", F.sum("amount").over(w3))
)

deduped = (
    with_windows
    .withColumn("rank_in_group", F.row_number().over(w4))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

with_windows_transactions = (
    deduped
    .select(
        "id",
        "txn_type",
        "amount",
        "txn_type_min_amount",
        "running_amount",
        F.row_number().over(w5).alias("row_seq"),
        F.row_number().over(w6).alias("event_seq"),
    )
)

kept_keys = (
    with_windows_transactions
    .select("id", "txn_type")
    .intersectAll(
        df_transactions
        .filter(F.col("txn_type").isin(["FEE", "REFUND", "DEBIT"]))
        .select("id", "txn_type")
    )
)

kept_rows = (
    with_windows_transactions
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.txn_type") == F.col("k.txn_type")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy("txn_type")
    .agg(
        F.percentile_approx("running_amount", [0.25, 0.5, 0.75]).alias("running_amount_quartiles"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.sum(F.when(F.col("txn_type") == "REFUND", F.col("txn_type_min_amount"))).alias("refund_total_txn_type_min_amount"),
        F.count("*").alias("event_count"),
        F.percentile_approx("txn_type_min_amount", 0.9).alias("txn_type_min_amount_p90"),
        F.sum(F.when(F.col("txn_type") == "DEBIT", F.col("running_amount"))).alias("debit_total_running_amount"),
    )
)
