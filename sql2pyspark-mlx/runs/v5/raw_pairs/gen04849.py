from pyspark.sql import functions as F

customer_summary = (
    df_accounts
    .filter(F.col("account_type") != "CREDIT")
    .groupBy("customer_id")
    .agg(
        F.sum(F.when(F.col("account_type") == "CHECKING", F.col("balance")).otherwise(0)).alias("checking_balance"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_summary.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "inner")
    .filter(F.col("cs.checking_balance") > 76)
    .select(F.col("c.is_active"), F.col("cs.checking_balance"))
)
