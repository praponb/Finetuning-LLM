from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("balance").desc(), "id")
w2 = Window.partitionBy("account_type").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())
w4 = Window.partitionBy("status").orderBy(F.col("distinct_status").desc())

enriched = (
    df_accounts
    .select(
        "id",
        "customer_id",
        "balance",
        "opened_date",
        "status",
        F.row_number().over(w).alias("row_seq"),
        F.last("balance", ignorenulls=True).over(w2).alias("last_known_balance"),
    )
)

base = (
    enriched
    .filter(F.col("status").isNotNull())
    .select(
        "id",
        "balance",
        "opened_date",
        "status",
        "row_seq",
        (F.col("balance") / F.nullif("last_known_balance", F.lit(0))).alias("balance_per_last_known_balance"),
    )
)

with_last = (
    base
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.balance",
        "s.opened_date",
        "s.status",
        "s.row_seq",
        "s.balance_per_last_known_balance",
        F.col("c.amount"),
    )
)

eligible = (
    with_last
    .alias("s")
    .join(df_transactions.alias("x"), (F.col("x.account_id") == F.col("s.id")) & (F.col("x.txn_time") >= F.col("s.opened_date")), "left_semi")
)

result = (
    eligible
    .groupBy(
        "status",
        F.date_trunc("YEAR", "opened_date").alias("year_start"),
        F.date_trunc("WEEK", "opened_date").alias("week_start"),
    )
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.round(F.sum("row_seq") / F.nullif(F.count(F.when(F.col("status") == "CLOSED", 1)), F.lit(0)), 2).alias("row_seq_per_closed"),
        F.count(F.when(F.col("status") == "OPEN", 1)).alias("open_rows"),
        F.count("*").alias("event_count"),
        F.countDistinct("status").alias("distinct_status"),
    )
    .withColumn("distinct_status_dense_rank", F.dense_rank().over(w4))
    .orderBy(F.col("open_rows").desc_nulls_first())
)
