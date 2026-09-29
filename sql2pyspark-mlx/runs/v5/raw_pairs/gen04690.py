from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w3 = Window.partitionBy("account_id", "txn_type").orderBy(F.col("merchant_length").desc())
w4 = Window.partitionBy("account_id").orderBy("amount", "id")
w5 = Window.partitionBy("txn_type", "account_id")
w6 = Window.partitionBy("status").orderBy(F.col("merchant_length_max_30d").asc_nulls_first())
w7 = Window.partitionBy("account_id", "txn_type")

base = (
    df_transactions
    .filter((F.col("txn_time") >= F.to_date(F.lit("2023-08-03"))) & F.col("merchant").isNotNull())
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "is_flagged",
        F.length("merchant").alias("merchant_length"),
        F.coalesce("merchant", F.lit("OTHER")).alias("resolved_merchant"),
    )
)

scored = (
    base
    .withColumn("running_amount", F.sum("amount").over(w))
    .withColumn("merchant_length_max_30d", F.max("merchant_length").over(w2))
    .withColumn("merchant_length_cume_dist", F.cume_dist().over(w3))
    .withColumn("prev_amount", F.lag("amount", 1, 0).over(w4))
)

enriched = (
    scored
    .alias("s")
    .join(df_accounts.alias("a"), F.col("s.account_id") == F.col("a.id"), "left")
    .select(
        "s.id",
        "s.account_id",
        "s.txn_type",
        "s.amount",
        "s.is_flagged",
        "s.merchant_length",
        "s.resolved_merchant",
        "s.merchant_length_max_30d",
        F.col("a.opened_date"),
        F.col("a.status"),
    )
)

kept_keys = (
    enriched
    .select("txn_type", "account_id")
    .subtract(
        df_transactions
        .filter(F.col("txn_time") < "2023-05-30")
        .select("txn_type", "account_id")
    )
)

reconciled = (
    enriched
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.txn_type") == F.col("k.txn_type")) & (F.col("s.account_id") == F.col("k.account_id")), "inner")
    .select("s.*")
)

metrics = (
    reconciled
    .filter(F.col("amount") >= 3700)
    .select(
        "id",
        "amount",
        "is_flagged",
        "merchant_length",
        "resolved_merchant",
        "opened_date",
        "status",
        F.min("merchant_length").over(w5).alias("txn_type_min_merchant_length"),
        F.dense_rank().over(w6).alias("merchant_length_max_30d_dense_rank"),
        F.count("merchant_length_max_30d").over(w7).alias("account_id_count_merchant_length_max_30d"),
    )
)

totals = (
    metrics
    .rollup("status")
    .agg(
        F.count(F.when(F.col("status") == "FROZEN", 1)).alias("frozen_count"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count(F.when(F.col("status") == "FROZEN", 1)).alias("frozen_rows"),
        F.grouping("status").alias("gid"),
    )
)

result = (
    totals
    .select(
        "status",
        "frozen_count",
        "distinct_ids",
        "gid",
        F.when(F.col("status").rlike("^(CLOSED|FROZEN)$"), "GROUP_B").when(F.col("status").rlike("^(OPEN)$"), "CORE").otherwise("GROUP_A").alias("status_zone"),
    )
    .orderBy(F.col("distinct_ids").desc_nulls_last())
)
