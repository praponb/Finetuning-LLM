from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(((F.col("txn_type") == "REFUND") & ~F.col("is_flagged")) | (F.col("txn_type") == "DEBIT"))
    .select("id", "txn_type", F.initcap(F.trim("txn_type")).alias("txn_type_title"))
)
