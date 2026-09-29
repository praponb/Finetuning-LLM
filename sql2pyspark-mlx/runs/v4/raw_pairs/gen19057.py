from pyspark.sql import functions as F

with_dims = (
    df_accounts
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.signup_date"), F.col("p.country"), F.col("p.segment"))
)

result = (
    with_dims
    .select(
        "id",
        "account_type",
        "opened_date",
        "status",
        "signup_date",
        "country",
        "segment",
    )
)
