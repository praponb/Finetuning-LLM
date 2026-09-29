from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.to_date("txn_time") < "2023-03-01") & F.col("amount").between(2500, 2700))
    .select("amount", F.hour("txn_time").alias("txn_time_hour"))
)
