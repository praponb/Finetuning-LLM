from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())

valid = (
    df_accounts
    .filter((F.col("opened_date") >= F.lit("2024-01-19").cast("date")) & F.col("balance").between(18000, 20000))
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        F.date_format("opened_date", "yyyy-MM").alias("opened_date_label"),
        F.concat_ws("-", "status", "account_type").alias("status_account_type_key"),
        F.coalesce("status", F.lit("N/A")).alias("resolved_status"),
    )
)

latest_match = (
    valid
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.txn_type"))
)

not_flagged = (
    latest_match
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.merchant").isin(["7-Eleven", "Starbucks", "Lazada"])), "left_semi")
)

result = (
    not_flagged
    .select(
        "id",
        "balance",
        "status",
        "status_account_type_key",
        "resolved_status",
        "txn_type",
    )
    .orderBy(F.col("id").desc_nulls_first())
)
