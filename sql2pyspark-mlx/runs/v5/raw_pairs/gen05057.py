from pyspark.sql import functions as F

kept_keys = (
    df_transactions
    .select("id", "txn_type")
    .intersectAll(
        df_transactions
        .filter(~F.col("is_flagged"))
        .select("id", "txn_type")
    )
)

reconciled = (
    df_transactions
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.txn_type") == F.col("k.txn_type")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .withColumn("year_start", F.date_trunc("YEAR", "txn_time"))
    .groupingSets(
        [["year_start", "merchant"], ["year_start"], []],
        "year_start",
        "merchant",
    )
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.min("txn_time").alias("first_txn_time"),
        F.size(F.collect_set("merchant")).alias("num_merchants"),
        F.grouping_id().alias("grouping_level"),
    )
    .filter(F.col("avg_amount") > 0)
)
