from pyspark.sql import functions as F

result = (
    df_accounts
    .select("status", "account_type")
    .subtract(
        df_accounts
        .filter(F.col("opened_date") < "2023-10-01")
        .select("status", "account_type")
    )
)
