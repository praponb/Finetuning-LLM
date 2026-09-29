from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("account_type").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.orderBy(F.col("balance").asc_nulls_first())
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w4 = Window.partitionBy("account_type").orderBy("balance_dense_rank")

with_windows = (
    df_accounts
    .withColumn("running_avg_balance", F.avg("balance").over(w_grp))
    .withColumn("running_min_balance", F.min("balance").over(w_grp))
    .withColumn("running_max_balance", F.max("balance").over(w_grp))
    .withColumn("running_sum_balance", F.sum("balance").over(w_grp))
    .withColumn("balance_dense_rank", F.dense_rank().over(w_part))
)

enriched = (
    with_windows
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id") == F.col("p.id"), "left")
    .select("e.*", F.col("p.name"), F.col("p.country"))
)

with_last = (
    enriched
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.txn_type"), F.col("c.amount").alias("transaction_amount"))
)

deduped = (
    with_last
    .withColumn("rn", F.rank().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "id",
        "customer_id",
        "running_min_balance",
        "running_max_balance",
        "balance_dense_rank",
        "name",
        "txn_type",
    )
)

kept = (
    deduped
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.account_id").isin([1, 11])), "left_anti")
)

with_dims = (
    kept
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id") == F.col("d.id"), "left")
    .filter(F.length(F.col("d.name")) < 12)
    .select(
        "a.*",
        F.col("d.city"),
        F.col("d.name").alias("customer_name"),
        F.col("d.email"),
    )
)

enriched_accounts = (
    with_dims
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id") == F.col("d.id"), "left")
    .select(
        "s.id",
        "s.customer_id",
        "s.running_max_balance",
        "s.balance_dense_rank",
        "s.txn_type",
        "s.city",
        F.col("d.email").alias("customer_email"),
        F.col("d.segment"),
        F.col("d.name").alias("customer_name_2"),
    )
)

summary = (
    enriched_accounts
    .rollup("txn_type")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.sort_array(F.collect_set("txn_type")).alias("txn_type_list"),
        F.round(F.sum("balance_dense_rank") / F.nullif(F.count(F.when(F.col("city") == "Bangkok", 1)), F.lit(0)), 2).alias("balance_dense_rank_per_bangkok"),
        F.grouping("txn_type").alias("gid"),
    )
    .filter(F.col("balance_dense_rank_per_bangkok") > 0)
)

result = summary.select("txn_type", "balance_dense_rank_per_bangkok")
