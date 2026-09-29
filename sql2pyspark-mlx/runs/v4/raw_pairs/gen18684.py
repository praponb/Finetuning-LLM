from pyspark.sql import functions as F

customer_summary = (
    df_accounts
    .filter(F.col("account_type") != "CREDIT")
    .groupBy("customer_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "FROZEN", 1).otherwise(0))) / F.count("*")), 1).alias("pct_frozen"),
        F.sum("balance").alias("total_balance"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_summary.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "inner")
    .filter(F.col("cs.total_balance") > 375000)
    .select(F.col("c.credit_limit"), F.col("cs.pct_frozen"), F.col("cs.total_balance"))
    .orderBy(F.col("total_balance").desc())
)
