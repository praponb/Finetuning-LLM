from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(((F.col("amount") <= 3100) & F.col("txn_type").isin(["FEE", "CREDIT", "REFUND"])) | (F.col("amount") < 2500))
    .groupBy(F.to_date("txn_time").alias("txn_day"))
    .agg(F.min("amount").alias("min_amount"))
    .filter(F.col("min_amount") < 1800)
    .orderBy("txn_day")
)
