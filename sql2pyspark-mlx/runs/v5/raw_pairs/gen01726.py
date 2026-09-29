from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isin([13, 19, 21, 30]) & (F.col("customer_id").isNotNull() | F.col("status").isNotNull()))
    .select(
        "id",
        "opened_date",
        "account_type",
        "status",
        F.coalesce("status", F.lit("unassigned")).alias("status_filled"),
    )
)
