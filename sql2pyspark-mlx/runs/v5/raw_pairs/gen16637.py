from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(~F.col("is_flagged"))
    .select("id", "amount", "txn_type")
    .orderBy(F.col("txn_type").desc(), "id")
)
