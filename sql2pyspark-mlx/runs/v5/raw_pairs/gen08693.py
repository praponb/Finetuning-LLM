from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("account_id").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w_running = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w3 = Window.orderBy("amount", "id")
w_recent = Window.orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("account_id").orderBy("amount", "id")
amount_by_account_id = df_transactions.groupBy("account_id").agg(F.max("amount").alias("max_transaction_amount"))

scored = (
    df_transactions
    .select(
        "*",
        F.sum("amount").over(w_part).alias("amount_sum_30d"),
        F.lead("amount", 2).over(w_running).alias("next_amount"),
        F.ntile(5).over(w3).alias("amount_bucket"),
        F.first("merchant", ignorenulls=True).over(w_recent).alias("first_known_merchant"),
        F.ntile(3).over(w5).alias("amount_bucket_2"),
    )
)

result = (
    scored
    .join(amount_by_account_id, on="account_id", how="left")
    .select(
        "id",
        "account_id",
        "txn_type",
        "merchant",
        "next_amount",
        "amount_bucket",
        "first_known_merchant",
        F.when(F.col("merchant").rlike("^(Starbucks|7-Eleven)$"), "GROUP_A").otherwise("OTHER_ZONE").alias("merchant_zone"),
        F.col("max_transaction_amount"),
    )
)
