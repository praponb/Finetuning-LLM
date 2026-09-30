from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(~F.col("is_flagged") & (F.col("txn_type") != "CREDIT") & F.col("merchant").isNotNull())
    .select("id", "txn_time")
)
