from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("customer_id")
    .agg(F.sum("balance").alias("total_balance"))
    .filter(F.col("total_balance") > 175000)
    .select("customer_id", "total_balance")
    .orderBy("customer_id")
)
