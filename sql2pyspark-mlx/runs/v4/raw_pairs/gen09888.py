from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .groupBy(F.quarter(F.col("c.signup_date")).alias("signup_quarter"))
    .agg(
        F.max(F.col("a.balance")).alias("max_balance"),
        F.sum(F.when(F.col("a.account_type") == "LOAN", F.col("a.balance")).otherwise(0)).alias("loan_balance"),
    )
    .orderBy("max_balance", "signup_quarter")
    .limit(5)
)
