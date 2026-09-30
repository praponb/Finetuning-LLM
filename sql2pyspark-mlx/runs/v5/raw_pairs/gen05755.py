from pyspark.sql import functions as F

result = (
    df_accounts
    .select("account_type")
    .subtract(
        df_accounts
        .filter(F.col("account_type").isin(["CHECKING", "LOAN", "CREDIT"]))
        .select("account_type")
    )
)
