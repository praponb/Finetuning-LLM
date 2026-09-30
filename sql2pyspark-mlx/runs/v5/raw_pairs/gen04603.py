from pyspark.sql import functions as F

result = (
    df_accounts
    .select("account_type", "status")
    .intersect(
        df_accounts
        .filter(F.col("account_type") == "CREDIT")
        .select("account_type", "status")
    )
)
