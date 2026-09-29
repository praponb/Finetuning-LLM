from pyspark.sql import functions as F

customer_summary = (
    df_accounts
    .groupBy("customer_id")
    .agg(
        F.min("balance").alias("min_balance"),
        (F.max("balance") - F.min("balance")).alias("balance_range"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_summary.alias("C"), F.col("c.id") == F.col("C.customer_id"), "inner")
    .filter(F.col("C.min_balance") >= 68000)
    .select(F.col("c.age"), F.col("C.min_balance"), F.col("C.balance_range"))
    .orderBy(F.col("balance_range").desc(), "min_balance")
)
