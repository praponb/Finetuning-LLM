from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type", "merchant").orderBy(F.col("amount").asc_nulls_last(), "id")
amount_by_account_id = df_transactions.groupBy("account_id").agg(F.min("amount").alias("min_transaction_amount"))

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
        "txn_time",
        "merchant",
        "latest_rank",
        F.when(F.col("merchant").rlike("^(Starbucks|Grab)$"), "CORE").when(F.col("merchant").rlike("^(7-Eleven|Amazon)$"), "EXTENDED").otherwise("GROUP_B").alias("merchant_zone"),
        F.col("min_transaction_amount"),
    )
)
