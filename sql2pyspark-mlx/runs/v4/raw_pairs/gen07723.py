from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("merchant").orderBy("amount", "id")
w3 = Window.partitionBy("account_id").orderBy(F.col("amount").asc_nulls_last())
w4 = Window.partitionBy("merchant").orderBy(F.col("amount").asc_nulls_last())
w5 = Window.partitionBy("merchant").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy(F.col("amount_dense_rank").desc(), "id")
w7 = Window.partitionBy("status").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("txn_type").orderBy(F.col("prev_amount").desc(), "id")
w9 = Window.partitionBy("account_type").orderBy("txn_time", "id")
w10 = Window.partitionBy("account_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_transactions
    .select(
        "*",
        F.sum("amount").over(w).alias("running_amount"),
        F.lag("amount", 1).over(w2).alias("prev_amount"),
        F.dense_rank().over(w3).alias("amount_dense_rank"),
        F.rank().over(w4).alias("amount_rank"),
        F.sum("amount").over(w5).alias("cumulative_amount"),
    )
)

enriched_transactions = (
    enriched
    .alias("src")
    .join(df_accounts.alias("b"), F.col("src.account_id") == F.col("b.id"), "left")
    .select("src.*", F.col("b.account_type"), F.col("b.status"))
)

valid = (
    enriched_transactions
    .filter((F.col("txn_time") >= F.lit("2023-09-06").cast("date")) & F.col("merchant").isNotNull() & (F.col("txn_type") == "DEBIT"))
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "is_flagged",
        "prev_amount",
        "amount_dense_rank",
        "cumulative_amount",
        "account_type",
        "status",
        F.when(F.col("merchant").isin(["Lazada", "Grab"]), "Tier 1").otherwise("Other").alias("merchant_group"),
        F.coalesce("account_type", F.lit("UNKNOWN")).alias("account_type_clean"),
    )
)

enriched_transactions_2 = (
    valid
    .withColumn("row_seq", F.row_number().over(w6))
    .withColumn("last_known_account_type", F.last("account_type", ignorenulls=True).over(w7))
)

deduped = (
    enriched_transactions_2
    .withColumn("dedup_rank", F.row_number().over(w8))
    .filter(F.col("dedup_rank") == 1)
)

with_prev = deduped.withColumn("prev_txn_time", F.lag("txn_time").over(w9))

with_gaps = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_txn_time").isNull() | (F.col("txn_time") > F.col("prev_txn_time") + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

islands = with_gaps.withColumn("session_no", F.sum("is_new_session").over(w10))

result = (
    islands
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "prev_amount",
        "account_type",
        "status",
        "merchant_group",
    )
    .orderBy(F.col("account_id").desc(), F.col("txn_time").desc())
)
