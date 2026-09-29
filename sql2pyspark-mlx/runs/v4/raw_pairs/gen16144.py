from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.to_date("txn_time") >= "2025-01-01")
    .orderBy(F.col("account_id").desc(), "id")
    .limit(10)
)
