from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(~F.col("status").isin(["FROZEN", "CLOSED"]) | F.col("account_type").isin(["CREDIT", "SAVINGS", "LOAN"]))
    .groupBy("status")
    .agg(F.countDistinct("balance").alias("unique_customers"))
    .orderBy(F.col("unique_customers").desc(), "status")
    .limit(50)
)
