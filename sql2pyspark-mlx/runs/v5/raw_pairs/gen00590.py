from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status").isin(["FROZEN", "OPEN"]) & F.col("customer_id").isin([9, 34, 39]))
    .select(
        "customer_id",
        "opened_date",
        "balance",
        F.abs("balance").alias("abs_balance"),
    )
)
