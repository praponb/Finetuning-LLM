from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("amount", "id")
w2 = Window.orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
amount_by_account_id = df_transactions.groupBy("account_id").agg(F.sum("amount").alias("sum_transaction_amount"))

enriched = (
    df_transactions
    .filter(F.col("account_id") == 1)
    .withColumn("seq_no", F.row_number().over(w))
    .withColumn("amount_bucket", F.ntile(4).over(w))
    .withColumn("amount_count_90d", F.count("amount").over(w2))
)

result = (
    enriched
    .join(amount_by_account_id, on="account_id", how="left")
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "merchant",
        "amount_bucket",
        "amount_count_90d",
        F.col("sum_transaction_amount"),
    )
    .orderBy(F.col("id").desc(), F.col("txn_type").desc())
)
