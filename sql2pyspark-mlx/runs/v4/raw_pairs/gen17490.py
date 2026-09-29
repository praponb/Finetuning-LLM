from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isin([20, 21, 36]))
    .select("status", F.lit("account").alias("source"))
    .unionByName(df_payments.select("status", F.lit("payment").alias("source")))
    .orderBy("status")
)
