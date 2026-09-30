from pyspark.sql import functions as F

accounts_by_customer = (
    df_accounts
    .filter(~F.col("account_type").isin(["CHECKING", "SAVINGS"]))
    .groupBy("customer_id")
    .agg(
        F.sum(F.when(F.col("status") == "CLOSED", F.col("balance")).otherwise(0)).alias("closed_balance"),
        F.round(((100 * F.sum(F.when(F.col("status") == "FROZEN", 1).otherwise(0))) / F.count("*")), 1).alias("pct_frozen"),
    )
)

result = (
    df_customers.alias("c")
    .join(accounts_by_customer.alias("t"), F.col("c.id") == F.col("t.customer_id"), "left")
    .filter(F.col("t.closed_balance") >= 20)
    .select(
        F.col("c.segment"),
        F.col("c.age"),
        F.col("t.closed_balance"),
        F.col("t.pct_frozen"),
    )
    .orderBy(F.col("age").desc())
)
