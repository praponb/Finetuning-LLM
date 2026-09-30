from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(-5, -1)
amount_by_account_id = df_transactions.groupBy("account_id").agg(F.avg("amount").alias("avg_transaction_amount"))

metrics = (
    df_transactions
    .filter(F.hour("txn_time").between(12, 20))
    .select(
        "*",
        F.sum("amount").over(w_grp).alias("running_amount"),
        F.avg("amount").over(w2).alias("amount_avg_5_prior"),
    )
)

result = (
    metrics
    .join(amount_by_account_id, on="account_id", how="left")
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "merchant",
        "is_flagged",
        "running_amount",
        "amount_avg_5_prior",
        F.col("avg_transaction_amount"),
    )
    .orderBy(F.col("id").desc_nulls_first(), F.col("amount").desc())
)
