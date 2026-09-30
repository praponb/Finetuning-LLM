from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id")
w2 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w4 = Window.orderBy(F.col("amount").desc_nulls_first())
w5 = Window.partitionBy("merchant")
w6 = Window.partitionBy("week_start").orderBy(F.col("distinct_ids").desc())

with_gaps = (
    df_transactions
    .withColumn("is_new_burst", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("burst_id", F.sum("is_new_burst").over(w2))

kept_keys = (
    sessionized
    .select("account_id", "merchant")
    .intersect(
        df_transactions
        .filter(F.col("account_id").isin([1, 15, 31]))
        .select("account_id", "merchant")
    )
)

reconciled = (
    sessionized
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.account_id") == F.col("k.account_id")) & (F.col("s.merchant") == F.col("k.merchant")), "inner")
    .select("s.*")
)

filtered = (
    reconciled
    .filter(F.col("account_id").isin([5, 7, 9, 12]))
    .withColumn("amount_per_burst_id", (F.col("amount") / F.nullif("burst_id", F.lit(0))))
    .withColumn("txn_type_merchant", F.concat("txn_type", F.lit(" "), "merchant"))
    .withColumn("resolved_merchant", F.coalesce("merchant", F.lit("UNKNOWN")))
)

enriched = (
    filtered
    .select(
        "*",
        F.sum("amount").over(w3).alias("amount_sum_30d"),
        F.dense_rank().over(w4).alias("amount_dense_rank"),
        F.max("amount_per_burst_id").over(w5).alias("merchant_max_amount_per_burst_id"),
    )
)

result = (
    enriched
    .groupBy(
        F.date_trunc("WEEK", "txn_time").alias("week_start"),
        F.when(F.col("amount_sum_30d") >= 20791, "HIGH").otherwise("LOW").alias("amount_sum_30d_tier"),
    )
    .agg(
        F.avg(F.when(F.col("merchant") == "7-Eleven", F.col("amount"))).alias("7_eleven_avg_amount"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .withColumn("distinct_ids_rank", F.rank().over(w6))
    .orderBy(F.col("distinct_ids").desc_nulls_first())
)
