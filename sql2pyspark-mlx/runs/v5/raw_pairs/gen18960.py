from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.filter(F.col("amount") < 2200).alias("t"), F.col("a.id") == F.col("t.account_id"), "left_semi")
    .filter(F.col("a.opened_date").between("2023-11-01", "2024-11-01"))
    .select(
        F.col("a.id"),
        F.col("a.customer_id"),
        F.col("a.account_type"),
        F.col("a.opened_date"),
    )
)
