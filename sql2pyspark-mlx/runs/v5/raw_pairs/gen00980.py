from pyspark.sql import functions as F

valid_accounts = df_accounts.filter(F.col("customer_id").isin([3, 17, 18, 34]))

summary = (
    valid_accounts
    .groupBy("account_type")
    .agg(
        F.sum(F.when(F.col("account_type") == "CREDIT", 1).otherwise(0)).alias("credit_count"),
    )
)

result = (
    summary
    .filter(F.col("credit_count") > 43)
    .orderBy(F.col("credit_count").desc(), "account_type")
    .limit(3)
)
