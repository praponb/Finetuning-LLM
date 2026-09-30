from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())

cleaned = (
    df_accounts
    .filter(F.col("status").isNotNull() & (F.col("customer_id") == 32))
    .withColumn("clean_account_type", F.trim("account_type"))
)

enriched = (
    cleaned
    .alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.segment").isin(["SMB", "Consumer", "Corporate"]))
    .select("s.*", F.col("c.city"))
)

scored = (
    enriched
    .select(
        "id",
        "account_type",
        "opened_date",
        "status",
        "clean_account_type",
        "city",
        F.avg("balance").over(w).alias("running_avg_balance"),
        F.count("balance").over(w).alias("running_count_balance"),
        F.max("balance").over(w).alias("running_max_balance"),
    )
)

with_last = (
    scored
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") <= F.col("s.opened_date")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.txn_type").alias("transaction_txn_type"))
)

daily = (
    with_last
    .withColumn("running_avg_balance_tier", F.when(F.col("running_avg_balance") >= 63000, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["running_avg_balance_tier", "account_type"], ["running_avg_balance_tier"]],
        "running_avg_balance_tier",
        "account_type",
    )
    .agg(
        F.round(F.sum("running_count_balance") / F.nullif(F.count(F.when(F.col("account_type") == "CREDIT", 1)), F.lit(0)), 2).alias("running_count_balance_per_credit"),
        F.percentile_approx("running_avg_balance", [0.25, 0.5, 0.75]).alias("running_avg_balance_quartiles"),
        F.grouping_id().alias("grouping_level"),
    )
    .filter(F.col("running_count_balance_per_credit") > 0)
)

result = (
    daily
    .select(
        "running_count_balance_per_credit",
        "running_avg_balance_quartiles",
        "grouping_level",
    )
    .orderBy("grouping_level", "running_count_balance_per_credit")
)
