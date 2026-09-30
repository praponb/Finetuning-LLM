from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("balance").desc_nulls_last(), "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("customer_id", "account_type")
w4 = Window.partitionBy("customer_id", "status").orderBy("days_since_opened_date", "id")
w5 = Window.partitionBy("status").orderBy("days_since_opened_date", "id")
w6 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())

valid = (
    df_accounts
    .filter(F.col("opened_date") > F.lit("2023-04-18").cast("date"))
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        F.datediff(F.current_date(), "opened_date").alias("days_since_opened_date"),
        F.coalesce("status", F.lit("UNKNOWN")).alias("status_clean"),
    )
)

latest = (
    valid
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

with_latest = (
    latest
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") <= F.col("s.opened_date")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.merchant").alias("transaction_merchant"),
        F.col("c.amount").alias("transaction_amount"),
    )
)

scored = (
    with_latest
    .withColumn("customer_id_stddev_samp_days_since_opened_date", F.stddev_samp("days_since_opened_date").over(w3))
    .withColumn("prev_transaction_amount", F.lag("transaction_amount", 1).over(w4))
    .withColumn("seq_no", F.row_number().over(w5))
)

filtered = (
    scored
    .filter((F.col("opened_date") >= F.lit("2023-03-30").cast("date")) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "days_since_opened_date",
        "status_clean",
        "latest_rank",
        "transaction_merchant",
        "customer_id_stddev_samp_days_since_opened_date",
        "prev_transaction_amount",
        "seq_no",
        F.lower("account_type").alias("account_type_lower"),
        F.coalesce("transaction_merchant", F.lit("NONE")).alias("transaction_merchant_clean"),
    )
)

with_latest_accounts = (
    filtered
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.account_type",
        "s.balance",
        "s.latest_rank",
        "s.transaction_merchant",
        "s.customer_id_stddev_samp_days_since_opened_date",
        "s.prev_transaction_amount",
        "s.seq_no",
        "s.account_type_lower",
        "s.transaction_merchant_clean",
        F.col("c.txn_time"),
    )
)

enriched = (
    with_latest_accounts
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .select("a.*", F.col("c.country"), F.col("c.city"), F.col("c.segment"))
)

result = (
    enriched
    .select("id", "city", F.col("balance").alias("BALANCE"), "prev_transaction_amount")
    .unpivot(
        ids=["id", "city"],
        values=["BALANCE", "prev_transaction_amount"],
        variableColumnName="measure",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
