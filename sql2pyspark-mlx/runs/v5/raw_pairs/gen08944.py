from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("opened_date") < "2024-11-04")
    .select(
        "opened_date",
        "customer_id",
        "status",
        F.initcap(F.trim("account_type")).alias("account_type_title"),
    )
    .orderBy(F.col("customer_id").desc())
)
