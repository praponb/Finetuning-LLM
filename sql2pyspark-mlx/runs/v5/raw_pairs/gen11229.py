from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency")
w2 = Window.orderBy(F.col("val").asc_nulls_first())
w3 = Window.partitionBy("currency").orderBy("val_rank")
w4 = Window.orderBy(F.col("val_rank_cume_dist").desc_nulls_last())
w5 = Window.partitionBy("currency").orderBy(F.col("val_rank_cume_dist").desc_nulls_last())

lines = (
    df_price_tiers
    .select("id", "effective_from", "currency", F.explode("discounts").alias("val"))
)

valid = (
    lines
    .filter(F.col("effective_from") >= F.to_date(F.lit("2024-05-14")))
    .withColumn("effective_from_dayofweek", F.dayofweek("effective_from"))
    .withColumn("val_scaled", F.round((F.col("val") * 0.9), 2))
)

scored = (
    valid
    .select(
        "id",
        "currency",
        "val_scaled",
        F.stddev_samp("val").over(w).alias("currency_stddev_samp_val"),
        F.avg("val").over(w).alias("currency_avg_val"),
        F.count("val").over(w).alias("currency_count_val"),
        F.rank().over(w2).alias("val_rank"),
    )
)

cleaned = (
    scored
    .filter(F.col("val_scaled") <= 42)
    .select(
        "id",
        "currency",
        "val_scaled",
        "currency_avg_val",
        "currency_count_val",
        "val_rank",
        F.col("val_scaled").cast("int").alias("val_scaled_int"),
        F.substring("currency", 1, 3).alias("currency_prefix"),
    )
)

enriched = (
    cleaned
    .withColumn("currency_count_val_share", F.round((F.col("currency_count_val") / F.sum("currency_count_val").over(w)), 4))
    .withColumn("val_rank_cume_dist", F.cume_dist().over(w3))
)

filtered = (
    enriched
    .filter(F.col("val_rank").between(3, 7))
    .withColumn("currency_count_val_str", F.col("currency_count_val").cast("double"))
    .withColumn("clean_currency", F.trim("currency"))
)

scored_price_tiers = (
    filtered
    .select(
        "*",
        F.percent_rank().over(w4).alias("val_rank_cume_dist_percent_rank"),
        F.percent_rank().over(w5).alias("val_rank_cume_dist_percent_rank_2"),
        F.stddev_samp("val_scaled").over(w).alias("currency_stddev_samp_val_scaled"),
    )
)

result = (
    scored_price_tiers
    .groupBy(
        F.when(F.col("val_rank_cume_dist_percent_rank_2") >= 0.85, "HIGH").otherwise("LOW").alias("val_rank_cume_dist_percent_rank_2_tier"),
    )
    .agg(
        F.avg(F.when(F.col("currency") == "EUR", F.col("val_rank_cume_dist_percent_rank"))).alias("eur_avg_val_rank_cume_dist_percent_rank"),
        F.percentile_approx("currency_stddev_samp_val_scaled", 0.95).alias("currency_stddev_samp_val_scaled_p95"),
        F.avg("val_rank_cume_dist_percent_rank").alias("avg_val_rank_cume_dist_percent_rank"),
    )
    .filter(F.col("eur_avg_val_rank_cume_dist_percent_rank") < 1000000)
    .orderBy("currency_stddev_samp_val_scaled_p95")
)
