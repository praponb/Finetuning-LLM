from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("account_type").orderBy("txn_time", "id")
w3 = Window.partitionBy("account_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())

latest_match = (
    df_accounts
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.account_type",
        "s.opened_date",
        "s.status",
        F.col("c.txn_time"),
        F.col("c.merchant"),
    )
)

gaps = (
    latest_match
    .withColumn("session_start_flag", F.when(F.col("txn_time") - F.lag("txn_time").over(w2) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

sessionized = (
    gaps
    .select(
        "id",
        "account_type",
        "opened_date",
        "status",
        "txn_time",
        "merchant",
        F.sum("session_start_flag").over(w3).alias("island_id"),
    )
)

cleaned = (
    sessionized
    .filter((F.col("opened_date") > F.lit("2023-06-27").cast("date")) & F.col("merchant").isNotNull())
    .withColumn("merchant_lower", F.lower("merchant"))
)

with_latest = (
    cleaned
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.account_type",
        "s.opened_date",
        "s.txn_time",
        "s.merchant_lower",
        F.col("c.txn_time").alias("transaction_txn_time"),
        F.col("c.amount"),
    )
)

with_last = (
    with_latest
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.amount").alias("transaction_amount"))
)

result = (
    with_last
    .select(
        "id",
        "account_type",
        "opened_date",
        "txn_time",
        "merchant_lower",
        "transaction_txn_time",
        "amount",
        "transaction_amount",
        F.when(F.col("account_type").rlike("^(SAVINGS|LOAN)$"), "GROUP_A").when(F.col("account_type").rlike("^(CHECKING|CREDIT)$"), "PRIORITY").otherwise("CORE").alias("account_type_zone"),
    )
)
