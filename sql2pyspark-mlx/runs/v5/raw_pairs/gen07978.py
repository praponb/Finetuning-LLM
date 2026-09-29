from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")
w2 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
amount_by_account_id = df_transactions.groupBy("account_id").agg(F.min("amount").alias("min_transaction_amount"))

flagged = (
    df_transactions
    .withColumn("new_group_flag", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessions_transactions = flagged.withColumn("session_no", F.sum("new_group_flag").over(w2))

result = (
    sessions_transactions
    .join(amount_by_account_id, on="account_id", how="left")
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        F.when(F.col("txn_type").rlike("^(CREDIT|REFUND)$"), "EXTENDED").when(F.col("txn_type").rlike("^(DEBIT|FEE)$"), "OTHER_ZONE").otherwise("GROUP_B").alias("txn_type_zone"),
        F.col("min_transaction_amount"),
    )
)
