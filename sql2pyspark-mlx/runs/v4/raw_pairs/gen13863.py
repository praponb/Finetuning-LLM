from pyspark.sql import functions as F

valid = (
    df_accounts
    .filter(F.col("status").isNotNull())
    .select(
        "id",
        "customer_id",
        "account_type",
        F.col("balance").cast("int").alias("balance_int"),
        F.upper("account_type").alias("account_type_upper"),
    )
)

result = (
    valid
    .groupBy("customer_id")
    .pivot("account_type", ["SAVINGS", "CHECKING"])
    .agg(F.max("balance_int").alias("amount"))
)
