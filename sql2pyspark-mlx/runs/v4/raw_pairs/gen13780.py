from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("opened_date") < "2025-03-11")
    .withColumn("balance_int", F.col("balance").cast("int"))
)
