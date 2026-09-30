from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("amount", "id")
w2 = Window.partitionBy("merchant").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
amount_by_account_id = df_transactions.groupBy("account_id").agg(F.max("amount").alias("max_transaction_amount"))

enriched = (
    df_transactions
    .withColumn("amount_bucket", F.ntile(5).over(w))
    .withColumn("last_known_merchant", F.last("merchant", ignorenulls=True).over(w2))
)

result = (
    enriched
    .join(amount_by_account_id, on="account_id", how="left")
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "last_known_merchant",
        F.col("max_transaction_amount"),
    )
    .orderBy(F.col("txn_type").desc_nulls_first(), F.col("txn_time").desc())
)
