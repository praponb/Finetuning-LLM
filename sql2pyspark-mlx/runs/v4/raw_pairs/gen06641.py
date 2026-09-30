from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(~F.col("account_type").isin(["LOAN", "CREDIT"]) & (F.col("account_type") == "SAVINGS"))
    .select(
        "id",
        "status",
        "opened_date",
        "account_type",
        F.coalesce("balance", F.lit(0)).alias("balance_filled"),
    )
)
