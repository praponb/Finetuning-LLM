from pyspark.sql import functions as F

result = (
    df_accounts
    .select(
        "balance",
        "opened_date",
        "customer_id",
        F.when(F.col("balance") >= 72000, "High").when(F.col("balance") >= 29000, "Medium").otherwise("Low").alias("balance_tier"),
    )
)
