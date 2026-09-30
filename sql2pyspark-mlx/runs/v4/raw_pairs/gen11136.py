from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "account_type").orderBy("opened_date", "id")
w2 = Window.partitionBy("status").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("account_type")
w4 = Window.orderBy("balance", "id")
w5 = Window.partitionBy("status").orderBy("account_type_sum_balance", "id").rowsBetween(-6, Window.currentRow)
w6 = Window.partitionBy("account_type").orderBy("opened_date", "id")
w7 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())

with_windows = (
    df_accounts
    .withColumn("event_seq", F.row_number().over(w))
    .withColumn("first_known_balance", F.first("balance", ignorenulls=True).over(w2))
    .withColumn("account_type_sum_balance", F.sum("balance").over(w3))
)

with_windows_accounts = (
    with_windows
    .select(
        "*",
        F.row_number().over(w4).alias("row_seq"),
        F.avg("account_type_sum_balance").over(w5).alias("account_type_sum_balance_avg_6"),
        F.lag("account_type_sum_balance", 1, 0).over(w6).alias("prev_account_type_sum_balance"),
    )
)

latest_match = (
    with_windows_accounts
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w7))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.txn_time").alias("transaction_txn_time"),
        F.col("c.amount").alias("transaction_amount"),
    )
)

with_dims = (
    latest_match
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .select("a.*", F.col("c.city"), F.col("c.segment"))
)

totals = (
    with_dims
    .groupBy(
        F.when(F.col("first_known_balance") >= 64000, "HIGH").otherwise("LOW").alias("first_known_balance_tier"),
    )
    .agg(
        F.sum(F.when(F.col("city") == "Bangkok", F.col("account_type_sum_balance_avg_6")).otherwise(0)).alias("bangkok_account_type_sum_balance_avg_6"),
        F.sum(F.when(F.col("account_type") == "CREDIT", F.col("event_seq"))).alias("credit_total_event_seq"),
        F.count(F.when(F.col("account_type") == "CHECKING", 1)).alias("checking_rows"),
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
        F.round(F.sum("first_known_balance") / F.nullif(F.count(F.when(F.col("status") == "OPEN", 1)), F.lit(0)), 2).alias("first_known_balance_per_open"),
        ((F.unix_timestamp(F.max("transaction_txn_time")) - F.unix_timestamp(F.min("transaction_txn_time"))) / 60).alias("span_minutes"),
    )
)

result = (
    totals
    .select(
        "first_known_balance_tier",
        "bangkok_account_type_sum_balance_avg_6",
        "checking_rows",
    )
)
