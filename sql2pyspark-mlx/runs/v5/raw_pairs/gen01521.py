from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant", "account_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("account_id")
amount_by_account_id = df_transactions.groupBy("account_id").agg(F.min("amount").alias("min_transaction_amount"))

scored = (
    df_transactions
    .filter(F.col("account_id").isin([15, 24, 32, 35]))
    .select(
        "id",
        "account_id",
        "txn_type",
        "txn_time",
        "merchant",
        "is_flagged",
        F.min("amount").over(w).alias("running_min_amount"),
        F.max("amount").over(w).alias("running_max_amount"),
        F.count("amount").over(w).alias("running_count_amount"),
        F.round((F.col("amount") / F.sum("amount").over(w2)), 4).alias("amount_share"),
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
        "is_flagged",
        "running_min_amount",
        "running_count_amount",
        "amount_share",
        F.when(F.col("merchant").rlike("^(Amazon|Lazada)$"), "OTHER_ZONE").otherwise("CORE").alias("merchant_zone"),
        F.col("min_transaction_amount"),
    )
    .orderBy(F.col("merchant_zone").desc_nulls_last())
)
