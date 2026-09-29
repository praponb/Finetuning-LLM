from pyspark.sql import functions as F

filtered = (
    df_accounts
    .filter((F.col("opened_date") >= F.lit("2023-05-12").cast("date")) & F.col("customer_id").isNull())
    .select(
        "id",
        "customer_id",
        "account_type",
        "opened_date",
        F.substring("account_type", 1, 2).alias("account_type_prefix"),
    )
)

result = (
    filtered
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.txn_time") < F.col("s.opened_date")), "left_anti")
)
