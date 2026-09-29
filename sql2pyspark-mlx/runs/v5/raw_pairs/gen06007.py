from pyspark.sql import functions as F

kept_keys = (
    df_payments
    .select("method", "id")
    .intersectAll(
        df_payments
        .filter(F.col("amount") <= 680)
        .select("method", "id")
    )
)

reconciled = (
    df_payments
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.method") == F.col("k.method")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .withColumn("amount_tier", F.when(F.col("amount") >= 1380, "PREMIUM").otherwise("STANDARD"))
    .rollup("amount_tier", "status")
    .agg(
        F.sort_array(F.collect_set("method")).alias("method_list"),
        F.avg("amount").alias("avg_amount"),
        F.grouping_id().alias("gid"),
    )
)
