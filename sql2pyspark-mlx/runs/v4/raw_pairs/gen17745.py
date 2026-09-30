from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency", "plan").orderBy(F.col("val").desc_nulls_first())
w2 = Window.partitionBy("currency").orderBy(F.col("val").asc_nulls_last())
w3 = Window.orderBy(F.col("val").asc_nulls_last())
w4 = Window.partitionBy("plan", "currency").orderBy(F.col("val").asc_nulls_first())
w5 = Window.partitionBy("currency")
w6 = Window.partitionBy("plan")
w7 = Window.partitionBy("currency", "plan").orderBy(F.col("val_cume_dist_2").desc_nulls_last())
w_running = Window.partitionBy("plan", "currency")

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode_outer("tier_prices").alias("val"),
    )
)

enriched = (
    flattened
    .withColumn("val_rank", F.rank().over(w))
    .withColumn("val_dense_rank", F.dense_rank().over(w2))
    .withColumn("val_cume_dist", F.cume_dist().over(w3))
    .withColumn("val_cume_dist_2", F.cume_dist().over(w4))
    .withColumn("currency_count_val", F.count("val").over(w5))
)

matched_keys = (
    enriched
    .select("id")
    .intersect(
        df_price_tiers
        .filter(F.col("effective_from") > "2025-01-01")
        .select("id")
    )
)

verified = (
    enriched
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

base = (
    verified
    .filter(F.col("plan") == "enterprise")
    .select(
        "*",
        F.concat_ws("/", "plan", "currency").alias("plan_currency_key"),
        (F.col("val_dense_rank") - F.col("val")).alias("val_dense_rank_minus_val"),
    )
)

metrics = (
    base
    .withColumn("plan_min_val", F.min("val").over(w6))
    .withColumn("val_cume_dist_2_rank", F.rank().over(w7))
)

cleaned = (
    metrics
    .filter(F.col("currency_count_val") == 14)
    .select(
        "*",
        (F.col("val_cume_dist_2") + F.col("val_cume_dist")).alias("val_cume_dist_2_plus_val_cume_dist"),
    )
)

with_windows = (
    cleaned
    .withColumn("currency_max_val_cume_dist_2_plus_val_cume_dist", F.max("val_cume_dist_2_plus_val_cume_dist").over(w5))
    .withColumn("currency_min_val_dense_rank_minus_val", F.min("val_dense_rank_minus_val").over(w5))
    .withColumn("plan_max_val_cume_dist_2_plus_val_cume_dist", F.max("val_cume_dist_2_plus_val_cume_dist").over(w_running))
)

result = (
    with_windows
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        "val",
        "val_rank",
        "val_dense_rank",
        "val_cume_dist",
        "currency_count_val",
        F.when(F.col("currency").rlike("^(EUR|THB)$"), "GROUP_A").otherwise("CORE").alias("currency_zone"),
    )
    .orderBy(F.col("currency").desc_nulls_first(), F.col("val_cume_dist").desc())
)
