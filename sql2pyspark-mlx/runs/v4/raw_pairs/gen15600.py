from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant", "account_id").orderBy(F.col("is_lazada").asc_nulls_first())
w2 = Window.orderBy(F.col("is_lazada").desc())
w3 = Window.partitionBy("merchant").orderBy("is_lazada")
w4 = Window.partitionBy("account_id").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w5 = Window.partitionBy("merchant").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)

base = (
    df_transactions
    .filter((F.col("txn_time") >= F.to_date(F.lit("2023-11-08"))) & F.col("merchant").isNotNull())
    .select(
        "id",
        "account_id",
        "amount",
        "txn_time",
        "merchant",
        F.when(F.col("merchant") == "Lazada", 1).otherwise(0).alias("is_lazada"),
    )
)

remaining_keys = (
    base
    .select("id", "account_id")
    .intersect(
        df_transactions
        .filter(F.col("amount") < 1600)
        .select("id", "account_id")
    )
)

reconciled = (
    base
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.account_id") == F.col("k.account_id")), "inner")
    .select("s.*")
)

with_windows = (
    reconciled
    .withColumn("is_lazada_dense_rank", F.dense_rank().over(w))
    .withColumn("is_lazada_cume_dist", F.cume_dist().over(w2))
)

with_windows_transactions = (
    with_windows
    .withColumn("is_lazada_rank", F.rank().over(w3))
    .withColumn("is_lazada_cume_dist_count_30d", F.count("is_lazada_cume_dist").over(w4))
    .withColumn("is_lazada_cume_dist_max_7d", F.max("is_lazada_cume_dist").over(w5))
)

totals = (
    with_windows_transactions
    .groupBy(
        F.when(F.col("is_lazada_cume_dist") >= 0.76, "PREMIUM").otherwise("STANDARD").alias("is_lazada_cume_dist_tier"),
    )
    .agg(
        F.min("txn_time").alias("first_txn_time"),
        F.round(F.avg("is_lazada"), 2).alias("avg_is_lazada"),
        F.size(F.collect_set("merchant")).alias("num_merchants"),
        F.count_if(F.col("merchant") == "Lazada").alias("lazada_events"),
    )
)

result = (
    totals
    .select(
        "is_lazada_cume_dist_tier",
        "first_txn_time",
        "avg_is_lazada",
        "num_merchants",
        "lazada_events",
    )
    .orderBy(F.col("is_lazada_cume_dist_tier").desc(), "lazada_events")
    .limit(20)
)
