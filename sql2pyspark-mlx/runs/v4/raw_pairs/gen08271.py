from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy("val")
w2 = Window.partitionBy("currency").orderBy(F.col("val").asc_nulls_first())
w3 = Window.partitionBy("currency")

lines = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode_outer("tier_limits").alias("val"),
    )
)

with_windows = (
    lines
    .withColumn("val_dense_rank", F.dense_rank().over(w))
    .withColumn("val_cume_dist", F.cume_dist().over(w2))
    .withColumn("currency_count_val", F.count("val").over(w3))
    .withColumn("currency_avg_val", F.avg("val").over(w3))
    .withColumn("currency_count_val_2", F.count("val").over(w3))
)

matched_keys = (
    with_windows
    .select("currency")
    .intersect(
        df_price_tiers
        .filter(F.col("effective_from").between("2025-06-01", "2025-10-05"))
        .select("currency")
    )
)

verified = (
    with_windows
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.currency") == F.col("k.currency")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy(F.date_trunc("quarter", "effective_from").alias("quarter_start"))
    .agg(
        F.round(F.sum("currency_avg_val") / F.nullif(F.count(F.when(F.col("currency") == "EUR", 1)), F.lit(0)), 2).alias("currency_avg_val_per_eur"),
        F.percentile_approx("val_cume_dist", 0.5).alias("val_cume_dist_p50"),
        F.sum(F.when(F.col("currency") == "EUR", F.col("currency_count_val")).otherwise(0)).alias("eur_currency_count_val"),
        F.percentile_approx("val_cume_dist", 0.5).alias("val_cume_dist_p50_2"),
    )
    .orderBy(F.col("val_cume_dist_p50_2").desc_nulls_first())
)
