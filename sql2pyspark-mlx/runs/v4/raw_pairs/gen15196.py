from pyspark.sql import functions as F

customer_totals = (
    df_accounts
    .groupBy("customer_id")
    .agg(
        F.count("*").alias("account_count"),
        F.min("balance").alias("min_balance"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_totals.alias("ct"), F.col("c.id") == F.col("ct.customer_id"), "left")
    .select(
        F.col("c.id"),
        F.col("c.name"),
        F.col("ct.account_count"),
        F.col("ct.min_balance"),
    )
    .orderBy(F.col("id").desc(), F.col("account_count").desc())
)
