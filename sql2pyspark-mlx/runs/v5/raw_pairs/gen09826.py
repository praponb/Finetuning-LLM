from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("customer_id").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("account_type", "status").orderBy("opened_date", "id")
w_ordered = Window.partitionBy("status").orderBy("last_known_balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("status", "account_type").orderBy(F.col("last_known_balance").desc_nulls_last())

eligible = (
    df_accounts
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.is_flagged")), "left_anti")
)

enriched = (
    eligible
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        F.sum("balance").over(w).alias("running_balance"),
        F.last("balance", ignorenulls=True).over(w2).alias("last_known_balance"),
        F.row_number().over(w3).alias("seq_no"),
    )
)

scored = (
    enriched
    .filter(F.col("account_type").isin(["LOAN", "CREDIT", "SAVINGS"]))
    .withColumn("running_count_running_balance", F.count("running_balance").over(w_ordered))
    .withColumn("running_max_running_balance", F.max("running_balance").over(w_ordered))
    .withColumn("running_avg_running_balance", F.avg("running_balance").over(w_ordered))
    .withColumn("last_known_balance_cume_dist", F.cume_dist().over(w_part))
)

remaining_keys = (
    scored
    .select("status", "customer_id")
    .exceptAll(
        df_accounts
        .filter(F.col("status") != "FROZEN")
        .select("status", "customer_id")
    )
)

kept_rows = (
    scored
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        "last_known_balance",
        "seq_no",
        "running_count_running_balance",
    )
    .orderBy(F.col("opened_date").asc_nulls_first())
)
