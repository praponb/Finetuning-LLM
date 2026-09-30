from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(-3, -1)
w2 = Window.partitionBy("merchant").orderBy("amount", "id").rowsBetween(-2, Window.currentRow)
amount_by_account_id = df_transactions.groupBy("account_id").agg(F.sum("amount").alias("sum_transaction_amount"))

scored = (
    df_transactions
    .filter(F.col("amount").between(2000, 2800))
    .withColumn("amount_avg_3_prior", F.avg("amount").over(w))
    .withColumn("amount_avg_2", F.avg("amount").over(w2))
)

result = (
    scored
    .join(amount_by_account_id, on="account_id", how="left")
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "amount_avg_2",
        F.col("sum_transaction_amount"),
    )
    .orderBy(F.col("account_id").desc_nulls_last())
)
