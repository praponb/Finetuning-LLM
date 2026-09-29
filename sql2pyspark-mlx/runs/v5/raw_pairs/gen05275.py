from pyspark.sql import functions as F

base = (
    df_accounts
    .filter(F.col("status").isNotNull())
    .select(
        "id",
        "customer_id",
        "account_type",
        "status",
        F.datediff(F.current_date(), "opened_date").alias("days_since_opened_date"),
    )
)

result = (
    base
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id") == F.col("p.id"), "inner")
    .select(
        "s.id",
        "s.customer_id",
        "s.account_type",
        "s.status",
        "s.days_since_opened_date",
        F.col("p.credit_limit"),
    )
)
