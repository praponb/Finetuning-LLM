from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("account_id") == 27) & F.hour("txn_time").between(9, 15))
    .select("id", "is_flagged")
    .orderBy("id")
)
