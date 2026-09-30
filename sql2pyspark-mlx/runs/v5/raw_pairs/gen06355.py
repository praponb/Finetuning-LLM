from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("txn_type", "customer_id").orderBy(F.col("balance").asc_nulls_first())
w3 = Window.partitionBy("txn_type").orderBy("balance", "id").rowsBetween(-3, Window.currentRow)
w_running = Window.partitionBy("txn_type").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("status").orderBy("balance", "id").rowsBetween(-2, Window.currentRow)

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
        "s.balance",
        "s.status",
        F.col("c.txn_type"),
    )
)

eligible = (
    latest_match
    .alias("s")
    .join(df_transactions.alias("x"), (F.col("x.account_id") == F.col("s.id")) & (F.col("x.amount") < 2600), "left_anti")
)

with_windows = (
    eligible
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "account_type",
        F.dense_rank().over(w2).alias("balance_dense_rank"),
        F.avg("balance").over(w3).alias("balance_avg_3"),
        F.sum("balance").over(w_running).alias("running_balance"),
        F.avg("balance").over(w_part).alias("balance_avg_2"),
    )
)

kept_keys = (
    with_windows
    .select("id")
    .intersect(
        df_accounts
        .filter(F.col("status").isin(["OPEN", "CLOSED"]))
        .select("id")
    )
)

kept_rows = (
    with_windows
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

kept = (
    kept_rows
    .alias("s")
    .join(df_transactions.alias("x"), (F.col("x.account_id") == F.col("s.id")) & (F.col("x.txn_type").isin(["DEBIT", "FEE"])), "left_semi")
)

enriched = (
    kept
    .alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .select(
        "s.id",
        "s.customer_id",
        "s.account_type",
        "s.balance_dense_rank",
        "s.balance_avg_2",
        F.col("c.credit_limit"),
        F.col("c.age"),
    )
)

kept_accounts = (
    enriched
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (~F.col("c.is_flagged")), "left_anti")
)

result = (
    kept_accounts
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .cube(
        F.col("c.country").alias("country"),
        F.col("c.segment").alias("segment"),
        F.col("a.account_type").alias("account_type"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.avg(F.col("a.credit_limit")).alias("avg_credit_limit"),
        F.grouping_id().alias("level_id"),
    )
)
