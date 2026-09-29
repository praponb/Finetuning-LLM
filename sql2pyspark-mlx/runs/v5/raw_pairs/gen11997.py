from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("currency_length").asc_nulls_first())
w2 = Window.partitionBy("currency").orderBy(F.col("val").asc_nulls_first())
w3 = Window.partitionBy("currency")

exploded = df_price_tiers.select("id", "currency", F.explode_outer("tier_prices").alias("val"))

base = (
    exploded
    .filter(F.col("val") > 47)
    .withColumn("currency_length", F.length("currency"))
    .withColumn("val_filled", F.coalesce("val", F.lit(0)))
)

enriched = (
    base
    .select(
        "*",
        F.dense_rank().over(w).alias("currency_length_dense_rank"),
        F.percent_rank().over(w2).alias("val_percent_rank"),
    )
)

remaining_keys = (
    enriched
    .select("id", "currency")
    .intersectAll(
        df_price_tiers
        .filter(F.col("effective_from") > "2024-04-01")
        .select("id", "currency")
    )
)

kept_rows = (
    enriched
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.currency") == F.col("k.currency")), "inner")
    .select("s.*")
)

with_windows = (
    kept_rows
    .select(
        "id",
        "currency",
        "val",
        "val_filled",
        "val_percent_rank",
        F.stddev_samp("currency_length_dense_rank").over(w3).alias("currency_stddev_samp_currency_length_dense_rank"),
    )
)

result = (
    with_windows
    .groupBy("currency")
    .agg(
        F.avg("val_percent_rank").alias("avg_val_percent_rank"),
        F.size(F.collect_set("currency")).alias("num_currencies"),
        F.count(F.when(F.col("currency") == "EUR", 1)).alias("eur_count"),
        F.percentile_approx("val", 0.9).alias("val_p90"),
        F.count(F.when(F.col("currency") == "THB", 1)).alias("thb_count"),
        F.sum(F.when(F.col("currency") == "EUR", F.col("val_percent_rank")).otherwise(0)).alias("eur_val_percent_rank"),
    )
)
