from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")
w2 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("txn_time", "id").rowsBetween(-6, -1)
w4 = Window.partitionBy("txn_type").orderBy("is_new_burst", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("account_id").orderBy(F.col("amount").desc(), "id")
w6 = Window.partitionBy("account_id", "txn_type")
w7 = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w8 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
amount_by_account_id = df_transactions.groupBy("account_id").agg(F.max("amount").alias("max_transaction_amount"))

base = (
    df_transactions
    .filter(F.col("account_id").isin([8, 29, 35, 36]) & F.col("merchant").isNotNull())
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "is_flagged",
        F.substring("merchant", 1, 3).alias("merchant_prefix"),
        F.lower("txn_type").alias("txn_type_lower"),
    )
)

gaps = (
    base
    .withColumn("is_new_burst", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

islands = gaps.withColumn("session_id", F.sum("is_new_burst").over(w2))

enriched = (
    islands
    .withColumn("session_id_avg_6_prior", F.avg("session_id").over(w3))
    .withColumn("running_session_id", F.sum("session_id").over(w4))
)

scored = (
    enriched
    .filter(F.col("session_id_avg_6_prior") != 2)
    .withColumn("row_seq", F.row_number().over(w5))
    .withColumn("amount_share", F.round((F.col("amount") / F.sum("amount").over(w6)), 4))
)

flagged = (
    scored
    .withColumn("is_new_session", F.when(F.lag("txn_time").over(w7).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w7) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("is_new_session").over(w8))

with_dims = (
    sessionized
    .alias("a")
    .join(df_accounts.alias("p2"), F.col("a.account_id") == F.col("p2.id"), "left")
    .select("a.*", F.col("p2.balance"), F.col("p2.account_type"))
)

result = (
    with_dims
    .join(amount_by_account_id, on="account_id", how="left")
    .select(
        "id",
        "account_id",
        "txn_type",
        "txn_time",
        "is_flagged",
        "merchant_prefix",
        "is_new_burst",
        "session_id_avg_6_prior",
        "running_session_id",
        F.when(F.col("txn_type").rlike("^(CREDIT|REFUND)$"), "CORE").when(F.col("txn_type").rlike("^(DEBIT|FEE)$"), "OTHER_ZONE").otherwise("EXTENDED").alias("txn_type_zone"),
        F.col("max_transaction_amount"),
    )
    .orderBy(F.col("account_id").asc_nulls_last())
)
