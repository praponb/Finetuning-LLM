from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("merchant").isin(["Starbucks", "Grab", "Lazada"]) & (F.col("account_id").isin([8, 27, 29, 31]) | (F.col("txn_time") < "2023-08-20")))
    .withColumn("txn_time_hour", F.hour("txn_time"))
    .withColumn("amount_whole", F.ceil("amount"))
    .orderBy("merchant")
)
