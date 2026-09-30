from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("opened_date", "id").rowsBetween(-2, -1)
w2 = Window.partitionBy("customer_id", "status").orderBy("opened_date", "id")
w3 = Window.partitionBy("customer_id").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("status").orderBy(F.col("opened_date").desc())
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())

metrics = (
    df_accounts
    .select(
        "*",
        F.avg("balance").over(w).alias("balance_avg_2_prior"),
        F.lead("balance", 2).over(w2).alias("next_balance"),
        F.last("balance", ignorenulls=True).over(w3).alias("last_known_balance"),
    )
)

latest = (
    metrics
    .withColumn("row_num", F.rank().over(w4))
    .filter(F.col("row_num") == 1)
)

with_last = (
    latest
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.txn_type"))
)

kept = (
    with_last
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.txn_time") < F.col("s.opened_date")), "left_anti")
)

result = (
    kept
    .groupBy(
        F.date_trunc("QUARTER", "opened_date").alias("quarter_start"),
        F.date_trunc("WEEK", "opened_date").alias("week_start"),
        "txn_type",
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("next_balance", [0.25, 0.5, 0.75]).alias("next_balance_quartiles"),
        F.percentile_approx("balance", [0.25, 0.5, 0.75]).alias("balance_quartiles"),
        F.size(F.collect_set("account_type")).alias("num_account_types"),
        F.round(F.sum("last_known_balance") / F.nullif(F.count(F.when(F.col("status") == "CLOSED", 1)), F.lit(0)), 2).alias("last_known_balance_per_closed"),
    )
    .orderBy(F.col("txn_type").desc())
)
