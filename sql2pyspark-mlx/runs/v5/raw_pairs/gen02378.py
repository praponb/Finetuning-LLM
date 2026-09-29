from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type", "account_id").orderBy(F.col("amount").desc_nulls_last(), "id")
amount_by_account_id = df_transactions.groupBy("account_id").agg(F.avg("amount").alias("avg_transaction_amount"))

latest = (
    df_transactions
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
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
        "merchant",
        "is_flagged",
        F.col("avg_transaction_amount"),
    )
)
