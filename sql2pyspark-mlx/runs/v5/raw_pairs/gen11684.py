from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy(F.col("val").asc_nulls_last())
w2 = Window.partitionBy("currency")
w3 = Window.partitionBy("currency").orderBy(F.col("val").desc())
w4 = Window.orderBy(F.col("val_percent_rank_2").desc_nulls_first())
w5 = Window.partitionBy("currency").orderBy(F.col("currency_min_val_2").desc_nulls_last())
w6 = Window.orderBy(F.col("currency_max_val_percent_rank_2").desc())
w_part = Window.orderBy("val_percent_rank")

flattened = (
    df_price_tiers
    .select("id", "effective_from", "currency", F.explode("tier_limits").alias("val"))
)

cleaned = (
    flattened
    .filter((F.col("effective_from") >= F.lit("2024-02-27").cast("date")) & (F.col("currency") == "USD"))
    .withColumn("currency_upper", F.upper("currency"))
)

metrics = (
    cleaned
    .select(
        "*",
        F.percent_rank().over(w).alias("val_percent_rank"),
        F.min("val").over(w2).alias("currency_min_val"),
        F.percent_rank().over(w3).alias("val_percent_rank_2"),
    )
)

with_windows = (
    metrics
    .filter(F.col("val").between(21, 59))
    .withColumn("currency_min_val_2", F.min("val").over(w2))
    .withColumn("currency_min_val_percent_rank_2", F.min("val_percent_rank_2").over(w2))
    .withColumn("currency_max_val_percent_rank_2", F.max("val_percent_rank_2").over(w2))
    .withColumn("val_percent_rank_2_percent_rank", F.percent_rank().over(w4))
)

scored = (
    with_windows
    .select(
        "currency",
        "currency_min_val",
        "val_percent_rank_2",
        "currency_min_val_percent_rank_2",
        F.dense_rank().over(w5).alias("currency_min_val_2_dense_rank"),
        F.rank().over(w6).alias("currency_max_val_percent_rank_2_rank"),
        F.cume_dist().over(w_part).alias("val_percent_rank_cume_dist"),
    )
)

valid = (
    scored
    .filter(F.col("currency_min_val_percent_rank_2") <= 0)
    .select(
        "currency",
        "currency_min_val",
        "val_percent_rank_2",
        "currency_min_val_percent_rank_2",
        "currency_min_val_2_dense_rank",
        "currency_max_val_percent_rank_2_rank",
        "val_percent_rank_cume_dist",
        F.round((F.col("val_percent_rank_2") / F.col("currency_min_val_2_dense_rank")), 2).alias("val_percent_rank_2_per_currency_min_val_2_dense_rank"),
    )
)

summary = (
    valid
    .groupBy("currency")
    .agg(
        F.count_if(F.col("currency") == "USD").alias("usd_events"),
        F.avg(F.when(F.col("currency") == "EUR", F.col("val_percent_rank_2_per_currency_min_val_2_dense_rank"))).alias("eur_avg_val_percent_rank_2_per_currency_min_val_2_dense_rank"),
        F.sort_array(F.collect_set("currency")).alias("currency_list"),
    )
    .filter(F.col("usd_events") >= 1)
)

result = (
    summary
    .select(
        "currency",
        "usd_events",
        "eur_avg_val_percent_rank_2_per_currency_min_val_2_dense_rank",
        F.when(F.col("currency").rlike("^(EUR|USD)$"), "CORE").when(F.col("currency").rlike("^(THB)$"), "OTHER_ZONE").otherwise("EXTENDED").alias("currency_zone"),
    )
    .orderBy("currency")
)
