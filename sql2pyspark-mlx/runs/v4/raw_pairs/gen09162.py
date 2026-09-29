from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())

valid = (
    df_accounts
    .filter(F.col("customer_id").isNotNull() & (F.col("account_type") == "SAVINGS"))
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        F.date_format("opened_date", "yyyy-MM-dd").alias("opened_date_label"),
        F.coalesce("status", F.lit("UNKNOWN")).alias("status_filled"),
    )
)

enriched = (
    valid
    .alias("s")
    .join(df_customers.alias("b"), F.col("s.customer_id") == F.col("b.id"), "left")
    .select("s.*", F.col("b.email"), F.col("b.city"))
)

enriched_accounts = (
    enriched
    .alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .select("s.*", F.col("c.signup_date"))
)

with_last = (
    enriched_accounts
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.txn_type").alias("transaction_txn_type"))
)

totals = (
    with_last
    .groupBy(
        F.when(F.col("balance") >= 87000, "PREMIUM").otherwise("STANDARD").alias("balance_tier"),
    )
    .agg(
        F.percentile_approx("balance", 0.95).alias("balance_p95"),
        F.count("*").alias("cnt"),
    )
)

result = (
    totals
    .select("balance_tier", "balance_p95")
    .orderBy(F.col("balance_p95").desc_nulls_first(), "balance_tier")
)
