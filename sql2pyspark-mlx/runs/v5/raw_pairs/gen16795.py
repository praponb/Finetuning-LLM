from pyspark.sql import functions as F

result = (
    df_transactions
    .select("id", "amount", F.initcap(F.trim("txn_type")).alias("txn_type_title"))
)
