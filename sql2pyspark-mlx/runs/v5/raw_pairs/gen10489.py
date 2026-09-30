from pyspark.sql import functions as F

valid = (
    df_accounts
    .filter(F.col("status").isNotNull() & F.col("customer_id").isin([20, 36]))
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        F.upper("account_type").alias("account_type_upper"),
        F.upper("status").alias("status_upper"),
    )
)

result = (
    valid
    .groupBy("status")
    .pivot("account_type", ["CREDIT", "SAVINGS", "CHECKING"])
    .agg(F.sum("balance"))
    .select(
        "status",
        F.col("CREDIT").alias("credit"),
        F.col("SAVINGS").alias("savings"),
        F.col("CHECKING").alias("checking"),
    )
)
