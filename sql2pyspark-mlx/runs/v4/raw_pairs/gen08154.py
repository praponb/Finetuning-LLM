from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(~((F.col("account_type") == "SAVINGS") | (F.col("account_type") != "CHECKING")))
    .groupBy("customer_id", "account_type")
    .agg(
        (F.max("balance") - F.min("balance")).alias("balance_range"),
        F.round(F.stddev("balance"), 2).alias("stddev_balance"),
    )
)
