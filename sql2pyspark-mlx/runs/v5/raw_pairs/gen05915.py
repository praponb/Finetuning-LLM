from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("is_flagged") & ~F.col("is_flagged")) | F.col("merchant").isin(["Shell", "7-Eleven", "Lazada"]))
    .select("id", "txn_type", "account_id", "merchant")
)
