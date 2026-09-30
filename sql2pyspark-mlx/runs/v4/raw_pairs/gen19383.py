from pyspark.sql import functions as F

cleaned = (
    df_accounts
    .filter(~F.col("status").isin(["OPEN", "CLOSED"]))
    .select(
        "id",
        "customer_id",
        "balance",
        "opened_date",
        "status",
        F.last_day("opened_date").alias("opened_date_month_end"),
        F.coalesce("status", F.lit("DIRECT")).alias("status_clean"),
    )
)

result = (
    cleaned
    .alias("s")
    .join(df_transactions.alias("x"), (F.col("x.account_id") == F.col("s.id")) & (F.col("x.txn_time") < F.col("s.opened_date_month_end")), "left_anti")
)
