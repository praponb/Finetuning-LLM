from pyspark.sql import functions as F

valid = (
    df_accounts
    .filter(F.col("status").isin(["CLOSED", "FROZEN"]))
    .select(
        "id",
        "customer_id",
        "balance",
        "opened_date",
        "status",
        F.round((F.col("balance") * 1.07), 2).alias("balance_adjusted"),
    )
)

result = (
    valid
    .select("customer_id", "status")
    .intersect(
        df_accounts
        .filter(F.col("status") == "FROZEN")
        .select("customer_id", "status")
    )
)
