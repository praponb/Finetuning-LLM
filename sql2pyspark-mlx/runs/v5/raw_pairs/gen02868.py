from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isNull())
    .withColumn("status_group", F.when(F.col("status").isin(["CLOSED", "OPEN"]), "Priority").otherwise("Other"))
)
