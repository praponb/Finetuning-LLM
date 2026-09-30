from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(~F.col("account_type").isin(["SAVINGS", "CHECKING"]))
    .select(
        "id",
        "balance",
        "customer_id",
        "status",
        F.round((F.col("balance") * 100), 2).alias("adjusted_balance"),
    )
)
