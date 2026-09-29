from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")
w2 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("account_id").orderBy("balance", "id")

filtered = (
    df_transactions
    .filter((F.col("txn_time") > F.lit("2023-10-06").cast("date")) & (~F.col("merchant").eqNullSafe(F.lit("Grab"))))
    .select(
        "id",
        "account_id",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        F.col("amount").cast("int").alias("amount_int"),
    )
)

flagged = (
    filtered
    .withColumn("session_start_flag", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

sessionized = (
    flagged
    .select(
        "id",
        "account_id",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        F.sum("session_start_flag").over(w2).alias("session_id"),
    )
)

enriched = (
    sessionized
    .alias("e")
    .join(df_accounts.alias("b"), F.col("e.account_id") == F.col("b.id"), "inner")
    .select(
        "e.id",
        "e.account_id",
        "e.merchant",
        "e.is_flagged",
        "e.session_id",
        F.col("b.status"),
        F.col("b.balance"),
    )
)

enriched_transactions = (
    enriched
    .alias("src")
    .join(df_accounts.alias("d"), F.col("src.account_id") == F.col("d.id"), "left")
    .select(
        "src.id",
        "src.account_id",
        "src.session_id",
        "src.status",
        "src.balance",
        F.col("d.balance").alias("account_balance"),
        F.col("d.status").alias("account_status"),
    )
)

ranked = (
    enriched_transactions
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
    .select("id", "account_id", "status", "balance", "account_status")
)

result = (
    ranked
    .groupBy(
        F.when(F.col("balance") >= 76000, "HIGH").otherwise("LOW").alias("balance_tier"),
    )
    .agg(
        F.percentile_approx("balance", [0.25, 0.5, 0.75]).alias("balance_quartiles"),
        F.avg("balance").alias("avg_balance"),
        F.size(F.collect_set("account_status")).alias("num_account_status"),
        F.sum(F.when(F.col("status") == "CLOSED", F.col("balance")).otherwise(0)).alias("closed_balance"),
        F.sum(F.when(F.col("account_status") == "CLOSED", F.col("balance")).otherwise(0)).alias("closed_balance_2"),
        F.sum(F.when(F.col("account_status") == "CLOSED", F.col("balance")).otherwise(0)).alias("closed_balance_3"),
    )
    .orderBy(F.col("num_account_status").desc())
)
