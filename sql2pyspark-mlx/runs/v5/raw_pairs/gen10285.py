from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("is_flagged") & (F.col("txn_type") == "CREDIT"))
    .select(
        "id",
        "account_id",
        "txn_type",
        F.when(F.col("is_flagged"), "Yes").otherwise("No").alias("is_flagged_label"),
        F.col("amount").cast("int").alias("amount_int"),
    )
)
