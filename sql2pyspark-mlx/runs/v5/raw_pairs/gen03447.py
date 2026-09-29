from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("customer_id").orderBy(F.col("balance").desc(), "id")
w3 = Window.orderBy(F.col("balance").desc(), "id")
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())

with_dims = (
    df_accounts
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id") == F.col("d.id"), "inner")
    .select(
        "e.id",
        "e.customer_id",
        "e.balance",
        "e.opened_date",
        "e.status",
        F.col("d.country"),
        F.col("d.age"),
        F.col("d.signup_date"),
    )
)

with_windows = (
    with_dims
    .select(
        "id",
        "customer_id",
        "opened_date",
        "status",
        "country",
        F.sum("balance").over(w).alias("cumulative_balance"),
        F.row_number().over(w_part).alias("event_seq"),
        F.ntile(3).over(w3).alias("balance_bucket"),
    )
)

not_flagged = (
    with_windows
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.account_id").isin([6, 26, 29])), "left_anti")
)

latest_match = (
    not_flagged
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.amount").alias("transaction_amount"), F.col("c.txn_type"))
)

totals = (
    latest_match
    .groupBy(
        F.date_trunc("week", "opened_date").alias("week_start"),
        F.when(F.col("transaction_amount") >= 3600, "HIGH").otherwise("LOW").alias("transaction_amount_tier"),
    )
    .agg(
        F.size(F.collect_set("status")).alias("num_status"),
        F.count(F.when(F.col("status") == "CLOSED", 1)).alias("closed_count"),
    )
    .filter(F.col("num_status") > 1)
)

result = totals.select("week_start", "transaction_amount_tier", "num_status", "closed_count")
