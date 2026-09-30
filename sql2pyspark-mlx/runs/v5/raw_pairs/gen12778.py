from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("balance", "id").rowsBetween(-5, Window.currentRow)
w2 = Window.partitionBy("account_type").orderBy("opened_date", "id")
w3 = Window.partitionBy("status")
w4 = Window.partitionBy("country")
w5 = Window.partitionBy("country").orderBy(F.col("balance").desc(), "id")
w_running = Window.partitionBy("status").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w8 = Window.partitionBy("transaction_txn_type").orderBy(F.col("running_max_balance_bucket").desc_nulls_last(), "id")

kept = (
    df_accounts
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.txn_time") < "2023-08-01"), "left_anti")
)

enriched = (
    kept
    .alias("e")
    .join(df_customers.alias("b"), F.col("e.customer_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.email"), F.col("b.name"), F.col("b.country"))
)

with_windows = (
    enriched
    .filter(F.col("balance").between(51000, 57000))
    .select(
        "id",
        "account_type",
        "opened_date",
        "status",
        "email",
        "country",
        F.avg("balance").over(w).alias("balance_avg_5"),
        F.lag("balance", 2).over(w2).alias("prev_balance"),
        F.min("balance").over(w3).alias("status_min_balance"),
        F.max("balance").over(w4).alias("country_max_balance"),
        F.ntile(3).over(w5).alias("balance_bucket"),
    )
)

scored = (
    with_windows
    .select(
        "id",
        "email",
        "prev_balance",
        F.count("balance_bucket").over(w_running).alias("running_count_balance_bucket"),
        F.sum("balance_bucket").over(w_running).alias("running_sum_balance_bucket"),
        F.max("balance_bucket").over(w_running).alias("running_max_balance_bucket"),
    )
)

with_latest = (
    scored
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w7))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.amount"), F.col("c.txn_type").alias("transaction_txn_type"))
)

ranked = (
    with_latest
    .withColumn("row_num", F.row_number().over(w8))
    .filter(F.col("row_num") == 1)
)

result = (
    ranked
    .cube(
        "transaction_txn_type",
        F.when(F.col("running_sum_balance_bucket") >= 34, "HIGH").otherwise("LOW").alias("running_sum_balance_bucket_tier"),
    )
    .agg(
        F.round(F.avg("prev_balance"), 2).alias("avg_prev_balance"),
        F.size(F.collect_set("transaction_txn_type")).alias("num_transaction_txn_types"),
        F.sort_array(F.collect_set("transaction_txn_type")).alias("transaction_txn_type_list"),
        F.max(F.when(F.col("transaction_txn_type") == "DEBIT", F.col("running_sum_balance_bucket"))).alias("debit_max_running_sum_balance_bucket"),
        F.sort_array(F.collect_set("transaction_txn_type")).alias("transaction_txn_type_list_2"),
        F.round(F.avg("prev_balance"), 2).alias("avg_prev_balance_2"),
    )
    .orderBy(F.col("num_transaction_txn_types").desc_nulls_first())
)
