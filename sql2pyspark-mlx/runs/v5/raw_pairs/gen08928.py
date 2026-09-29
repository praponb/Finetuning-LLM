from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy(F.col("amount").desc(), "id")
amount_by_account_id = df_transactions.groupBy("account_id").agg(F.avg("amount").alias("avg_transaction_amount"))

latest = (
    df_transactions
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "account_id", "txn_type", "amount", "txn_time")
)

result = (
    latest
    .join(amount_by_account_id, on="account_id", how="left")
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        F.when(F.col("txn_type").rlike("^(CREDIT|DEBIT)$"), "PRIORITY").when(F.col("txn_type").rlike("^(REFUND|FEE)$"), "EXTENDED").otherwise("CORE").alias("txn_type_zone"),
        F.col("avg_transaction_amount"),
    )
)
