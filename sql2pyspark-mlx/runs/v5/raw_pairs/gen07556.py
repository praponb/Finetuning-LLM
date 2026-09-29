from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")
w2 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("account_id").orderBy(F.col("island_id_p95").desc())

filtered = (
    df_transactions
    .filter(F.col("merchant").isNotNull())
    .select(
        "*",
        F.round((F.col("amount") * 100), 2).alias("amount_scaled"),
        F.coalesce("merchant", F.lit("NONE")).alias("resolved_merchant"),
    )
)

gaps = (
    filtered
    .withColumn("is_new_burst", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

islands = gaps.withColumn("island_id", F.sum("is_new_burst").over(w2))

result = (
    islands
    .groupBy(
        "account_id",
        F.when(F.col("is_new_burst") >= 1, "HIGH").otherwise("LOW").alias("is_new_burst_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("merchant")).alias("merchant_list"),
        F.percentile_approx("island_id", 0.95).alias("island_id_p95"),
        F.sort_array(F.collect_set("txn_type")).alias("txn_type_list"),
    )
    .withColumn("island_id_p95_dense_rank", F.dense_rank().over(w3))
)
