from pyspark.sql import functions as F

accounts_by_customer = (
    df_accounts
    .groupBy("customer_id")
    .agg(
        F.sum(F.when(F.col("account_type") == "CHECKING", 1).otherwise(0)).alias("checking_count"),
        F.round(F.avg("balance"), 2).alias("avg_balance"),
    )
)

result = (
    df_customers.alias("c")
    .join(accounts_by_customer.alias("t"), F.col("c.id") == F.col("t.customer_id"), "left")
    .filter(F.col("t.checking_count") >= 73)
    .select(
        F.col("c.id"),
        F.col("c.city"),
        F.col("c.signup_date"),
        F.col("t.checking_count"),
        F.col("t.avg_balance"),
    )
    .orderBy(F.col("signup_date").desc())
)
