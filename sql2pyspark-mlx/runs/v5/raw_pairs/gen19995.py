from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("txn_type")
    .agg(F.countDistinct("amount").alias("unique_merchants"))
    .filter(F.col("unique_merchants") >= 4)
    .orderBy(F.col("unique_merchants").desc())
)
