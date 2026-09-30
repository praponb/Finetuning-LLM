from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("txn_type", "customer_id").orderBy(F.col("signup_date").desc_nulls_first(), "id")

valid = (
    df_accounts
    .filter((~F.col("status").eqNullSafe(F.lit("OPEN"))) & (F.col("status") == "OPEN"))
    .select("*", F.upper("account_type").alias("account_type_upper"))
)

with_last = (
    valid
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.txn_time").alias("transaction_txn_time"))
)

with_latest = (
    with_last
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.account_type",
        "s.status",
        "s.transaction_txn_time",
        F.col("c.txn_type"),
    )
)

joined = (
    with_latest
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id") == F.col("p.id"), "left")
    .select(
        "src.id",
        "src.customer_id",
        "src.status",
        "src.transaction_txn_time",
        "src.txn_type",
        F.col("p.email"),
        F.col("p.signup_date"),
    )
)

deduped = (
    joined
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
)

result = (
    deduped
    .withColumn("week_start", F.date_trunc("week", "transaction_txn_time"))
    .rollup("week_start", "status")
    .agg(
        F.count_if(F.col("txn_type") == "REFUND").alias("refund_events"),
        F.count("*").alias("cnt"),
        F.count_if(F.col("txn_type") == "FEE").alias("fee_events"),
        F.grouping("week_start").alias("grouping_level"),
    )
    .orderBy(F.col("refund_events").desc_nulls_last())
)
