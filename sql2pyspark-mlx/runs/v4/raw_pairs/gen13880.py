from pyspark.sql import functions as F

accounts_by_customer = (
    df_accounts
    .groupBy("customer_id")
    .agg(
        F.min("balance").alias("min_balance"),
        F.count(F.when(F.col("status") == "OPEN", 1)).alias("open_count"),
    )
)

result = (
    df_customers.alias("c")
    .join(accounts_by_customer.alias("t"), F.col("c.id") == F.col("t.customer_id"), "left")
    .filter(F.col("t.open_count") >= 9)
    .select(
        F.col("c.id"),
        F.col("c.credit_limit"),
        F.col("c.country"),
        F.col("t.min_balance"),
        F.col("t.open_count"),
    )
    .orderBy("country")
)
