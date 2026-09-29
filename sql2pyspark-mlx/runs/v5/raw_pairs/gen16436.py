from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("balance", "id")
w2 = Window.orderBy("balance_whole", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status").orderBy("balance_whole", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("status").orderBy("balance_times_balance_whole", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("customer_id").orderBy("balance_times_balance_whole", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

eligible = (
    df_accounts
    .alias("s")
    .join(df_transactions.alias("x"), (F.col("x.account_id") == F.col("s.id")) & (F.col("x.txn_time") <= F.col("s.opened_date")), "left_semi")
)

valid = (
    eligible
    .filter(F.col("status").isNotNull() & (F.col("opened_date") >= F.to_date(F.lit("2023-05-28"))))
    .withColumn("balance_whole", F.floor("balance"))
)

eligible_accounts = (
    valid
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (~F.col("ch.is_flagged")), "left_anti")
)

not_flagged = (
    eligible_accounts
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") < F.col("s.opened_date")), "left_anti")
)

base = (
    not_flagged
    .filter(F.col("status").isNotNull() & (F.col("balance") <= 76000))
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        "balance_whole",
        (F.col("balance") * F.col("balance_whole")).alias("balance_times_balance_whole"),
        F.upper("status").alias("status_upper"),
    )
)

kept = (
    base
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.txn_time") <= F.col("s.opened_date")), "left_anti")
)

metrics = (
    kept
    .filter(F.col("opened_date").between("2023-06-01", "2025-05-01"))
    .select(
        "id",
        "balance",
        "opened_date",
        "status",
        "balance_whole",
        "balance_times_balance_whole",
        "status_upper",
        F.lag("balance_times_balance_whole", 1).over(w).alias("prev_balance_times_balance_whole"),
        F.first("status", ignorenulls=True).over(w2).alias("first_known_status"),
        F.sum("balance_times_balance_whole").over(w3).alias("cumulative_balance_times_balance_whole"),
        F.sum("balance").over(w4).alias("running_balance"),
        F.last("balance", ignorenulls=True).over(w5).alias("last_known_balance"),
    )
)

result = (
    metrics
    .rollup(
        "status",
        F.when(F.col("balance_times_balance_whole") >= 63, "PREMIUM").otherwise("STANDARD").alias("balance_times_balance_whole_tier"),
    )
    .agg(
        F.count(F.when(F.col("status") == "CLOSED", 1)).alias("closed_rows"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.round(F.avg("balance_whole"), 2).alias("avg_balance_whole"),
    )
)
