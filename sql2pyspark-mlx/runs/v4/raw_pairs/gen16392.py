from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance") >= 39000)
    .withColumn("balance_whole", F.floor("balance"))
    .withColumn("balance_int", F.col("balance").cast("int"))
    .orderBy(F.col("opened_date").desc())
)
