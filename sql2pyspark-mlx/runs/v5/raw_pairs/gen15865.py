from pyspark.sql import functions as F

valid = (
    df_accounts
    .filter(F.col("status").isin(["FROZEN", "OPEN", "CLOSED"]))
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        F.initcap(F.trim("account_type")).alias("account_type_title"),
    )
)

result = (
    valid
    .select("customer_id", "id")
    .intersectAll(
        df_accounts
        .filter(F.col("status") == "FROZEN")
        .select("customer_id", "id")
    )
)
