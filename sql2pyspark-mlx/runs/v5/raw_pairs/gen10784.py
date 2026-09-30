from pyspark.sql import functions as F

result = (
    df_accounts
    .select(
        "id",
        "account_type",
        "customer_id",
        F.initcap(F.trim("account_type")).alias("account_type_title"),
    )
)
