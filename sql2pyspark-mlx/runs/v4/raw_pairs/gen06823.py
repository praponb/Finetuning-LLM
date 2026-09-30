from pyspark.sql import functions as F

result = (
    df_customers.alias("customers")
    .join(df_accounts.alias("accounts"), F.col("accounts.customer_id") == F.col("customers.id"), "left")
    .filter(F.col("customers.segment").isin(["Corporate", "Consumer"]))
    .select(
        F.col("customers.city"),
        F.col("customers.name"),
        F.col("accounts.balance"),
        F.col("accounts.customer_id"),
    )
    .orderBy("balance")
)
