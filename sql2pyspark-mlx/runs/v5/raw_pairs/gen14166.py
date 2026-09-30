from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.to_date("txn_time") >= "2023-04-27") & F.col("is_flagged"))
    .select(
        "id",
        "amount",
        "account_id",
        "txn_type",
        F.when(F.col("txn_type") == "CREDIT", 1).otherwise(0).alias("is_credit"),
    )
    .orderBy("id", F.col("is_credit").desc())
    .limit(50)
)
