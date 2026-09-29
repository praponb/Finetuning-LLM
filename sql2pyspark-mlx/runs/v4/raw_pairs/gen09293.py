from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance") > 26000)
    .withColumn("clean_account_type", F.trim("account_type"))
    .withColumn("status_group", F.when(F.col("status").isin(["FROZEN", "OPEN"]), "Priority").otherwise("Other"))
)
