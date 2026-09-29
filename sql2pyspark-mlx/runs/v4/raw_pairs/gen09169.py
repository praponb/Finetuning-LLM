from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")
w2 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("account_id", "txn_type")
w5 = Window.partitionBy("account_id", "txn_type").orderBy("txn_time_date", "id")
w6 = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w7 = Window.partitionBy("account_id", "txn_type").orderBy(F.col("txn_time").desc(), "id")
amount_by_account_id = df_transactions.groupBy("account_id").agg(F.avg("amount").alias("avg_transaction_amount"))

valid = (
    df_transactions
    .filter(F.col("txn_time") > F.lit("2023-08-14").cast("date"))
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "is_flagged",
        F.to_date("txn_time").alias("txn_time_date"),
        F.concat_ws("-", "txn_type", "merchant").alias("txn_type_merchant_key"),
        F.coalesce("merchant", F.lit("NONE")).alias("merchant_filled"),
    )
)

with_gaps = (
    valid
    .withColumn("is_new_session", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("burst_id", F.sum("is_new_session").over(w2))

enriched = (
    sessionized
    .select(
        "*",
        F.sum("amount").over(w3).alias("running_amount"),
        F.count("amount").over(w4).alias("account_id_count_amount"),
        F.lead("burst_id", 1).over(w5).alias("next_burst_id"),
    )
)

flagged = (
    enriched
    .withColumn("is_new_burst", F.when(F.lag("txn_time").over(w6).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w6) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessions_transactions = flagged.withColumn("burst_id_2", F.sum("is_new_burst").over(w3))

latest = (
    sessions_transactions
    .withColumn("row_num", F.row_number().over(w7))
    .filter(F.col("row_num") == 1)
)

enriched_transactions = (
    latest
    .alias("a")
    .join(df_accounts.alias("p"), F.col("a.account_id") == F.col("p.id"), "inner")
    .select("a.*", F.col("p.status"), F.col("p.opened_date"), F.col("p.balance"))
)

result = (
    enriched_transactions
    .join(amount_by_account_id, on="account_id", how="left")
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "is_flagged",
        "txn_type_merchant_key",
        "is_new_session",
        "burst_id",
        F.col("avg_transaction_amount"),
    )
    .orderBy(F.col("is_new_session").desc())
)
