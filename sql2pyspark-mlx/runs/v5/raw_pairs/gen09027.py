from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("balance") < 75000) | F.col("status").isin(["CLOSED", "OPEN", "FROZEN"]))
    .withColumn("account_type_length", F.length("account_type"))
)
