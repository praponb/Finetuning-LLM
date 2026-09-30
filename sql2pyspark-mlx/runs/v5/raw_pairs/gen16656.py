from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant")
w2 = Window.orderBy(F.col("amount").asc_nulls_last())
w3 = Window.partitionBy("txn_type").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)

base = (
    df_transactions
    .filter((F.col("txn_time") > F.lit("2023-11-09").cast("date")) & F.col("merchant").isNotNull())
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        F.substring("txn_type", 1, 2).alias("txn_type_prefix"),
        F.trim("txn_type").alias("clean_txn_type"),
    )
)

remaining_keys = (
    base
    .select("id")
    .intersectAll(
        df_transactions
        .filter(F.col("is_flagged"))
        .select("id")
    )
)

kept_rows = (
    base
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

metrics = (
    kept_rows
    .withColumn("merchant_sum_amount", F.sum("amount").over(w))
    .withColumn("amount_cume_dist", F.cume_dist().over(w2))
    .withColumn("amount_count_7d", F.count("amount").over(w3))
)

enriched = (
    metrics
    .alias("e")
    .join(df_accounts.alias("d"), F.col("e.account_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.account_type"), F.col("d.balance"))
)

result = (
    enriched
    .withColumn("amount_cume_dist_tier", F.when(F.col("amount_cume_dist") >= 0.81, "PREMIUM").otherwise("STANDARD"))
    .withColumn("amount_tier", F.when(F.col("amount") >= 3100, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["amount_cume_dist_tier", "amount_tier"], ["amount_cume_dist_tier"], []],
        "amount_cume_dist_tier",
        "amount_tier",
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.countDistinct("account_id").alias("distinct_account_ids"),
        F.count(F.when(F.col("txn_type") == "REFUND", 1)).alias("refund_rows"),
        F.grouping("amount_cume_dist_tier").alias("grouping_id"),
    )
    .filter(F.col("num_rows") >= 1)
)
