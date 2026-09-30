from pyspark.sql import functions as F

result = (
    df_accounts
    .select(
        "balance",
        "status",
        F.initcap(F.trim("account_type")).alias("account_type_title"),
    )
)
