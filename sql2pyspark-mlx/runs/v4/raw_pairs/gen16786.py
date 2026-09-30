from pyspark.sql import functions as F

customer_stats = (
    df_accounts
    .groupBy("customer_id")
    .agg((F.max("balance") - F.min("balance")).alias("balance_range"))
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("C"), F.col("c.id") == F.col("C.customer_id"), "inner")
    .filter(F.col("C.balance_range") > 40)
    .select(F.col("c.signup_date"), F.col("c.city"), F.col("C.balance_range"))
    .orderBy("balance_range")
)
