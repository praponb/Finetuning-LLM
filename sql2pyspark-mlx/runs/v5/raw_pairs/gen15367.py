from pyspark.sql import functions as F

cleaned = (
    df_accounts
    .filter((F.col("opened_date") > F.lit("2023-05-06").cast("date")) & F.col("status").isNotNull())
    .select(
        "*",
        F.concat_ws("-", "account_type", "status").alias("account_type_status_key"),
    )
)

result = (
    cleaned
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id").eqNullSafe(F.col("c.id")), "inner")
    .select(
        "src.id",
        "src.customer_id",
        "src.balance",
        "src.opened_date",
        "src.account_type_status_key",
        F.col("c.email"),
        F.col("c.name"),
        F.col("c.country"),
    )
)
