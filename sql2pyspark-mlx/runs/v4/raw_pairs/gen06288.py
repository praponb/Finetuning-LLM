from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id", "merchant").orderBy(F.col("txn_time").desc(), "id")
amount_by_account_id = df_transactions.groupBy("account_id").agg(F.sum("amount").alias("sum_transaction_amount"))

latest = (
    df_transactions
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

result = (
    latest
    .join(amount_by_account_id, on="account_id", how="left")
    .select(
        "id",
        "account_id",
        "amount",
        "merchant",
        "is_flagged",
        F.col("sum_transaction_amount"),
    )
)
