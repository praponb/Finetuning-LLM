from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(~((F.col("status") != "OPEN") & (F.col("opened_date") <= "2024-05-01")))
    .select(
        "id",
        "status",
        "opened_date",
        "customer_id",
        F.coalesce("balance", F.lit(0)).alias("balance_filled"),
    )
)
