from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())

valid = (
    df_accounts
    .filter(F.col("customer_id").isNotNull() & F.col("customer_id").isNull())
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        F.date_sub("opened_date", 54).alias("opened_date_minus_54d"),
        F.coalesce("status", F.lit("N/A")).alias("status_clean"),
    )
)

latest_match = (
    valid
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.txn_time").alias("transaction_txn_time"))
)

with_dims = (
    latest_match
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id") == F.col("p.id"), "left")
    .select("src.*", F.col("p.segment"), F.col("p.name"), F.col("p.signup_date"))
)

wide = (
    with_dims
    .groupBy("account_type", "customer_id")
    .pivot("segment", ["Corporate", "SMB"])
    .agg(F.avg("balance").alias("amount"), F.count(F.lit(1)).alias("cnt"))
    .select(
        "account_type",
        "customer_id",
        F.col("Corporate_amount").alias("corporate_amount"),
        F.col("Corporate_cnt").alias("corporate_cnt"),
        F.col("SMB_amount").alias("smb_amount"),
        F.col("SMB_cnt").alias("smb_cnt"),
    )
)

result = (
    wide
    .select(
        "account_type",
        "customer_id",
        "corporate_amount",
        "corporate_cnt",
        "smb_amount",
        "smb_cnt",
    )
    .orderBy("smb_amount")
)
