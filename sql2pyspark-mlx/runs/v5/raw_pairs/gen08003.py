from pyspark.sql import functions as F

result = (
    df_accounts
    .select("status", "account_type")
    .subtract(
        df_accounts
        .filter(F.col("account_type") == "CREDIT")
        .select("status", "account_type")
    )
)
