from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("status").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("account_type", "customer_id").orderBy(F.col("balance").desc(), "id")
w3 = Window.partitionBy("account_type").orderBy("opened_date", "id").rowsBetween(-6, Window.currentRow)
w4 = Window.partitionBy("account_type").orderBy("running_avg_opened_date_month", "id")

cleaned = (
    df_accounts
    .filter((F.col("opened_date") >= F.lit("2023-09-23").cast("date")) & F.col("status").isNotNull())
    .select(
        "*",
        F.month("opened_date").alias("opened_date_month"),
        F.coalesce("status", F.lit("UNKNOWN")).alias("status_filled"),
    )
)

eligible = (
    cleaned
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.is_flagged")), "left_semi")
)

enriched = (
    eligible
    .filter(F.col("opened_date_month") > 67)
    .select(
        "*",
        F.max("opened_date_month").over(w_running).alias("running_max_opened_date_month"),
        F.sum("opened_date_month").over(w_running).alias("running_sum_opened_date_month"),
        F.avg("opened_date_month").over(w_running).alias("running_avg_opened_date_month"),
        F.row_number().over(w_hist).alias("event_seq"),
    )
)

eligible_accounts = (
    enriched
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.txn_time") <= F.col("s.opened_date")), "left_semi")
)

enriched_accounts = (
    eligible_accounts
    .withColumn("event_seq_avg_6", F.avg("event_seq").over(w3))
    .withColumn("next_event_seq", F.lead("event_seq", 1).over(w4))
)

result = (
    enriched_accounts
    .groupBy("customer_id")
    .pivot("status", ["OPEN", "FROZEN", "CLOSED"])
    .agg(F.sum("running_sum_opened_date_month"))
)
