from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w2 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("txn_type", "account_id").orderBy("burst_id", "id")
w4 = Window.partitionBy("account_id", "txn_type").orderBy("balance", "id").rowsBetween(-5, -1)
w5 = Window.partitionBy("account_id", "txn_type").orderBy("txn_time", "id").rowsBetween(-6, Window.currentRow)
w6 = Window.partitionBy("txn_type").orderBy(F.col("balance").asc_nulls_first(), "id")

base = (
    df_transactions
    .filter((F.col("txn_time") > F.to_date(F.lit("2023-08-10"))) & (~F.col("merchant").eqNullSafe(F.lit("Grab"))))
    .select(
        "*",
        F.when(F.col("txn_type").isin(["FEE", "CREDIT"]), "Core").otherwise("Other").alias("txn_type_group"),
        F.when(F.col("merchant").isin(["Shell", "Starbucks"]), "Tier 1").otherwise("Other").alias("merchant_group"),
    )
)

with_dims = (
    base
    .alias("e")
    .join(df_accounts.alias("b"), F.col("e.account_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.opened_date"))
)

flagged = (
    with_dims
    .withColumn("new_group_flag", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

islands = flagged.withColumn("burst_id", F.sum("new_group_flag").over(w2))

with_dims_transactions = (
    islands
    .alias("e")
    .join(df_accounts.alias("b"), F.col("e.account_id") == F.col("b.id"), "left")
    .filter(F.col("b.customer_id").isin([5, 19, 21, 37]))
    .select(
        "e.id",
        "e.account_id",
        "e.txn_type",
        "e.txn_time",
        "e.new_group_flag",
        "e.burst_id",
        F.col("b.opened_date").alias("account_opened_date"),
        F.col("b.balance"),
    )
)

scored = (
    with_dims_transactions
    .select(
        "id",
        "account_id",
        "txn_type",
        "new_group_flag",
        "account_opened_date",
        "balance",
        F.row_number().over(w3).alias("event_seq"),
        F.avg("new_group_flag").over(w4).alias("new_group_flag_avg_5_prior"),
        F.avg("new_group_flag").over(w5).alias("new_group_flag_avg_6"),
    )
)

latest = (
    scored
    .withColumn("dedup_rank", F.row_number().over(w6))
    .filter(F.col("dedup_rank") == 1)
)

matched_keys = (
    latest
    .select("account_id", "id")
    .intersectAll(
        df_transactions
        .filter(F.col("account_id") == 32)
        .select("account_id", "id")
    )
)

reconciled = (
    latest
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.account_id") == F.col("k.account_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy("account_id")
    .pivot("txn_type", ["FEE", "DEBIT"])
    .agg(F.sum("new_group_flag").alias("amount"), F.count(F.lit(1)).alias("n"))
    .select(
        "account_id",
        F.col("FEE_amount").alias("fee_amount"),
        F.col("FEE_n").alias("fee_n"),
        F.col("DEBIT_amount").alias("debit_amount"),
        F.col("DEBIT_n").alias("debit_n"),
    )
)
