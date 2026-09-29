from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w_recent = Window.partitionBy("country").orderBy(F.col("balance").desc())
w_recent_2 = Window.partitionBy("customer_id").orderBy(F.col("credit_limit").desc_nulls_first())
w4 = Window.partitionBy("transaction_merchant")

with_latest = (
    df_accounts
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.merchant").alias("transaction_merchant"))
)

not_flagged = (
    with_latest
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") >= F.col("s.opened_date")), "left_anti")
)

remaining_keys = (
    not_flagged
    .select("id", "customer_id")
    .subtract(
        df_accounts
        .filter(F.col("balance") > 55000)
        .select("id", "customer_id")
    )
)

kept_rows = (
    not_flagged
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

base = (
    kept_rows
    .filter(F.col("balance").isNotNull())
    .withColumn("status_prefix", F.substring("status", 1, 4))
    .withColumn("opened_date_plus_56d", F.date_add("opened_date", 56))
    .withColumn("status_filled", F.coalesce("status", F.lit("NONE")))
)

with_dims = (
    base
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id") == F.col("p.id"), "left")
    .select(
        "a.customer_id",
        "a.balance",
        "a.opened_date",
        "a.transaction_merchant",
        "a.status_prefix",
        "a.status_filled",
        F.col("p.country"),
        F.col("p.email"),
        F.col("p.credit_limit"),
    )
)

metrics = (
    with_dims
    .filter(F.col("customer_id").isNull())
    .select(
        "*",
        F.cume_dist().over(w_recent).alias("balance_cume_dist"),
        F.percent_rank().over(w_recent_2).alias("credit_limit_percent_rank"),
        F.round((F.col("credit_limit") / F.sum("credit_limit").over(w4)), 4).alias("credit_limit_share"),
    )
)

kept_keys = (
    metrics
    .select("customer_id")
    .intersect(
        df_accounts
        .filter(F.col("customer_id") == 2)
        .select("customer_id")
    )
)

verified = (
    metrics
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

result = (
    verified
    .rollup(
        F.when(F.col("credit_limit_percent_rank") >= 0.64, "LARGE").otherwise("SMALL").alias("credit_limit_percent_rank_tier"),
    )
    .agg(
        F.countDistinct("transaction_merchant").alias("distinct_transaction_merchants"),
        F.count("*").alias("event_count"),
        F.stddev_samp("balance").alias("balance_stddev"),
    )
)
