from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status").isin(["OPEN", "FROZEN", "CLOSED"]) & (F.col("status") != "OPEN"))
    .withColumn("status_length", F.length("status"))
    .withColumn("account_type_lower", F.lower("account_type"))
)
