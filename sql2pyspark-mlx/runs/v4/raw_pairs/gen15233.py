from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(~F.col("merchant").isin(["Lazada", "Amazon", "Starbucks"]))
    .withColumn("amount_int", F.col("amount").cast("int"))
    .orderBy(F.col("amount_int").desc())
)
