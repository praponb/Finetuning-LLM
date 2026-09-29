from pyspark.sql import functions as F

accounts_by_customer = (
    df_accounts
    .groupBy("customer_id")
    .agg(
        F.sum(F.when(F.col("account_type") == "SAVINGS", F.col("balance")).otherwise(0)).alias("savings_balance"),
        F.round(((100 * F.sum(F.when(F.col("status") == "CLOSED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_closed"),
    )
)

result = (
    df_customers.alias("c")
    .join(accounts_by_customer.alias("A"), F.col("c.id") == F.col("A.customer_id"), "inner")
    .select(
        F.col("c.id"),
        F.col("c.credit_limit"),
        F.col("c.signup_date"),
        F.col("A.savings_balance"),
        F.col("A.pct_closed"),
    )
    .orderBy(F.col("id").desc())
)
