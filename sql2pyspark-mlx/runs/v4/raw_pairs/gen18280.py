from pyspark.sql import functions as F

result = (
    df_accounts
    .withColumn("balance_level", F.when(F.col("balance") >= 61000, "High").when(F.col("balance") >= 33000, "Medium").otherwise("Low"))
    .orderBy(F.col("balance").desc())
)
