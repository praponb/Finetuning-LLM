from pyspark.sql import functions as F

customer_totals = (
    df_accounts
    .groupBy("customer_id")
    .agg(
        F.max("opened_date").alias("latest_opened_date"),
        (F.max("balance") - F.min("balance")).alias("balance_range"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_totals.alias("ct"), F.col("c.id") == F.col("ct.customer_id"), "left")
    .filter(F.col("ct.balance_range") >= 56)
    .select(
        F.col("c.id"),
        F.col("c.email"),
        F.col("c.age"),
        F.col("ct.latest_opened_date"),
        F.col("ct.balance_range"),
    )
)
