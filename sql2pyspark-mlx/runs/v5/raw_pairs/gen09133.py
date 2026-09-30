from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("opened_date").desc(), "id")
w2 = Window.partitionBy("status").orderBy(F.col("balance_filled").desc(), "id")
w_recent = Window.partitionBy("status").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_accounts
    .filter((F.col("opened_date") > F.to_date(F.lit("2023-06-17"))) & (F.col("status") != "CLOSED"))
    .select(
        "id",
        "customer_id",
        "opened_date",
        "status",
        F.coalesce("balance", F.lit(0)).alias("balance_filled"),
        F.coalesce("status", F.lit("DIRECT")).alias("status_clean"),
    )
)

not_flagged = (
    valid
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") < "2023-10-01"), "left_anti")
)

metrics = (
    not_flagged
    .withColumn("seq_no", F.row_number().over(w))
    .withColumn("row_seq", F.row_number().over(w2))
)

enriched = (
    metrics
    .filter(F.col("row_seq").isNotNull())
    .select(
        "id",
        "opened_date",
        "balance_filled",
        "status_clean",
        F.avg("balance_filled").over(w_recent).alias("running_avg_balance_filled"),
        F.sum("balance_filled").over(w_recent).alias("running_sum_balance_filled"),
        F.max("balance_filled").over(w_recent).alias("running_max_balance_filled"),
    )
)

result = (
    enriched
    .groupBy(
        F.date_trunc("QUARTER", "opened_date").alias("quarter_start"),
        F.when(F.col("balance_filled") >= 68, "HIGH").otherwise("LOW").alias("balance_filled_tier"),
    )
    .agg(
        F.count("*").alias("event_count"),
        F.percentile_approx("balance_filled", [0.25, 0.5, 0.75]).alias("balance_filled_quartiles"),
        F.percentile_approx("running_max_balance_filled", [0.25, 0.5, 0.75]).alias("running_max_balance_filled_quartiles"),
    )
    .orderBy("quarter_start")
)
