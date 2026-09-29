from pyspark.sql import functions as F

result = (
    df_accounts
    .select(
        "balance",
        "account_type",
        F.coalesce("status", F.lit("unassigned")).alias("status_filled"),
    )
)
