from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")
w2 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
amount_by_account_id = df_transactions.groupBy("account_id").agg(F.max("amount").alias("max_transaction_amount"))

flagged = (
    df_transactions
    .withColumn("is_new_burst", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("is_new_burst").over(w2))

result = (
    sessionized
    .join(amount_by_account_id, on="account_id", how="left")
    .select(
        "id",
        "account_id",
        "txn_type",
        "merchant",
        F.when(F.col("txn_type").rlike("^(REFUND|DEBIT)$"), "OTHER_ZONE").otherwise("CORE").alias("txn_type_zone"),
        F.col("max_transaction_amount"),
    )
    .orderBy(F.col("txn_type_zone").desc_nulls_first(), F.col("txn_type").desc())
)
