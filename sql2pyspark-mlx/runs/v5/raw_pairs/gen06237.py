from pyspark.sql import functions as F

base = (
    df_accounts
    .filter((F.col("opened_date") > F.to_date(F.lit("2023-04-12"))) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "account_type",
        "status",
        F.substring("account_type", 1, 4).alias("account_type_prefix"),
    )
)

result = (
    base
    .select("account_type", "id")
    .subtract(
        df_accounts
        .filter(F.col("customer_id") == 8)
        .select("account_type", "id")
    )
)
