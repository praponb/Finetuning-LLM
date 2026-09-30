from pyspark.sql import functions as F

accounts_by_customer = (
    df_accounts
    .filter(F.col("status").isNull())
    .groupBy("customer_id")
    .agg(
        F.countDistinct("balance").alias("unique_statuss"),
        F.round(F.avg("balance"), 2).alias("avg_balance"),
    )
)

result = (
    df_customers.alias("c")
    .join(accounts_by_customer.alias("t"), F.col("c.id") == F.col("t.customer_id"), "inner")
    .select(F.col("c.name"), F.col("t.unique_statuss"), F.col("t.avg_balance"))
)
