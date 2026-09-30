from pyspark.sql import functions as F

base = (
    df_accounts
    .filter(F.col("customer_id").isNotNull() & (F.col("opened_date") > F.to_date(F.lit("2023-05-15"))))
    .select(
        "*",
        F.upper("account_type").alias("account_type_upper"),
        F.abs("balance").alias("abs_balance"),
    )
)

result = (
    base
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id").eqNullSafe(F.col("c.id")), "inner")
    .select(
        "src.id",
        "src.customer_id",
        "src.account_type",
        "src.balance",
        "src.status",
        "src.account_type_upper",
        F.col("c.name"),
        F.col("c.email"),
        F.col("c.signup_date"),
    )
)
