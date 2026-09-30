from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w2 = Window.orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)

valid = (
    df_transactions
    .filter((F.col("txn_time") >= F.lit("2023-08-26").cast("date")) & (~F.col("merchant").eqNullSafe(F.lit("7-Eleven"))))
    .select(
        "*",
        F.when(F.col("amount") >= 3100, "High").when(F.col("amount") >= 1400, "Medium").otherwise("Low").alias("amount_tier"),
        F.when(F.col("txn_type").isin(["CREDIT", "REFUND"]), "Tier 1").otherwise("Other").alias("txn_type_group"),
    )
)

matched_keys = (
    valid
    .select("merchant")
    .intersectAll(
        df_transactions
        .filter(F.col("amount") < 1700)
        .select("merchant")
    )
)

verified = (
    valid
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.merchant") == F.col("k.merchant")), "inner")
    .select("s.*")
)

remaining_keys = (
    verified
    .select("merchant", "txn_type")
    .intersectAll(
        df_transactions
        .filter(F.col("account_id").isin([31, 32]))
        .select("merchant", "txn_type")
    )
)

verified_transactions = (
    verified
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.merchant") == F.col("k.merchant")) & (F.col("s.txn_type") == F.col("k.txn_type")), "inner")
    .select("s.*")
)

with_dims = (
    verified_transactions
    .alias("src")
    .join(df_accounts.alias("b"), F.col("src.account_id") == F.col("b.id"), "left")
    .filter(F.col("b.account_type") != "CHECKING")
    .select("src.*", F.col("b.status"), F.col("b.opened_date"), F.col("b.balance"))
)

enriched = (
    with_dims
    .withColumn("amount_sum_7d", F.sum("amount").over(w))
    .withColumn("amount_avg_90d", F.avg("amount").over(w2))
)

daily = (
    enriched
    .groupBy("txn_type")
    .agg(
        F.count("*").alias("row_count"),
        F.size(F.collect_set("status")).alias("num_status"),
        F.percentile_approx("amount_avg_90d", [0.25, 0.5, 0.75]).alias("amount_avg_90d_quartiles"),
        F.sort_array(F.collect_set("txn_type")).alias("txn_type_list"),
    )
)

result = (
    daily
    .select("txn_type", "row_count", "amount_avg_90d_quartiles")
    .orderBy(F.col("row_count").asc_nulls_first())
)
