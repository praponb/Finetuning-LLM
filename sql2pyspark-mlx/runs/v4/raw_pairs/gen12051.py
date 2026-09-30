from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.hour("txn_time").between(12, 19) & F.col("account_id").isin([26, 31]))
    .select(
        "id",
        "merchant",
        "txn_type",
        F.initcap(F.trim("txn_type")).alias("txn_type_title"),
    )
)
