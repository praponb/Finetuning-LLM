from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type", "status").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("customer_id", "status").orderBy("balance", "id")
w3 = Window.partitionBy("account_type").orderBy("opened_date", "id").rowsBetween(-2, Window.currentRow)
w4 = Window.partitionBy("customer_id").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("account_type")
w_running = Window.partitionBy("customer_id").orderBy("opened_date", "id").rowsBetween(-3, -1)
w7 = Window.partitionBy("status", "customer_id").orderBy(F.col("next_balance").desc())
w8 = Window.partitionBy("status").orderBy(F.col("last_known_balance").desc(), "id")
w9 = Window.partitionBy("status")
w10 = Window.partitionBy("account_type").orderBy("cumulative_balance_avg_3_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

kept = (
    df_accounts
    .alias("s")
    .join(df_transactions.alias("x"), (F.col("x.account_id") == F.col("s.id")) & (F.col("x.txn_time") <= F.col("s.opened_date")), "left_semi")
)

enriched = (
    kept
    .select(
        "*",
        F.last("balance", ignorenulls=True).over(w).alias("last_known_balance"),
        F.lead("balance", 2).over(w_hist).alias("next_balance"),
        F.avg("balance").over(w3).alias("balance_avg_2"),
        F.sum("balance").over(w4).alias("cumulative_balance"),
    )
)

with_windows = (
    enriched
    .select(
        "*",
        F.sum("next_balance").over(w5).alias("account_type_sum_next_balance"),
        F.avg("cumulative_balance").over(w_running).alias("cumulative_balance_avg_3_prior"),
    )
)

deduped = (
    with_windows
    .withColumn("row_num", F.rank().over(w7))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

scored = (
    deduped
    .select(
        "*",
        F.row_number().over(w8).alias("row_seq"),
        F.sum("next_balance").over(w9).alias("status_sum_next_balance"),
        F.sum("account_type_sum_next_balance").over(w10).alias("running_account_type_sum_next_balance"),
    )
)

joined = (
    scored
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id") == F.col("d.id"), "left")
    .select("a.*", F.col("d.age"))
)

stats = (
    joined
    .groupBy(F.date_trunc("quarter", "opened_date").alias("quarter_start"))
    .agg(
        F.count(F.when(F.col("account_type") == "LOAN", 1)).alias("loan_rows"),
        F.sum(F.when(F.col("status") == "FROZEN", F.col("account_type_sum_next_balance")).otherwise(0)).alias("frozen_account_type_sum_next_balance"),
        F.sum(F.when(F.col("account_type") == "LOAN", F.col("status_sum_next_balance")).otherwise(0)).alias("loan_status_sum_next_balance"),
        F.count(F.when(F.col("status") == "CLOSED", 1)).alias("closed_rows"),
        F.countDistinct("account_type").alias("distinct_account_types"),
    )
    .filter(F.col("distinct_account_types") >= 1)
)

result = (
    stats
    .select(
        "quarter_start",
        "loan_rows",
        "frozen_account_type_sum_next_balance",
        "loan_status_sum_next_balance",
        "closed_rows",
        "distinct_account_types",
    )
)
