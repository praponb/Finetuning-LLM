from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("balance") >= 30000) & (F.col("balance") > 24000) & (F.col("balance") > 70000))
    .select(
        "id",
        "balance",
        "account_type",
        "customer_id",
        F.datediff(F.current_date(), "opened_date").alias("days_since_opened_date"),
        F.when(F.col("status") == "FROZEN", 1).otherwise(0).alias("is_frozen"),
    )
    .orderBy("balance")
)
