from pyspark.sql import functions as F

result = (
    df_accounts
    .select("customer_id", "account_type")
    .subtract(
        df_accounts
        .filter(F.col("account_type") == "CHECKING")
        .select("customer_id", "account_type")
    )
)
