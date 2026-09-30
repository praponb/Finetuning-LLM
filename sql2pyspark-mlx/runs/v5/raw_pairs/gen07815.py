from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("val").desc())
w_part = Window.partitionBy("currency").orderBy(F.col("val_scaled").desc())
w3 = Window.partitionBy("currency")
w4 = Window.partitionBy("currency").orderBy("val_scaled")

exploded = (
    df_price_tiers
    .select("id", "effective_from", "currency", F.explode("tier_prices").alias("val"))
)

base = (
    exploded
    .filter(F.col("effective_from") > F.to_date(F.lit("2024-04-09")))
    .withColumn("val_scaled", F.round((F.col("val") * 0.9), 2))
)

enriched = (
    base
    .select(
        "effective_from",
        "currency",
        F.percent_rank().over(w).alias("val_percent_rank"),
        F.rank().over(w_part).alias("val_scaled_rank"),
        F.avg("val").over(w3).alias("currency_avg_val"),
        F.percent_rank().over(w4).alias("val_scaled_percent_rank"),
    )
)

valid = (
    enriched
    .filter(F.col("effective_from") >= F.lit("2024-02-11").cast("date"))
    .select(
        "effective_from",
        "currency",
        "val_percent_rank",
        "val_scaled_percent_rank",
        (F.col("currency_avg_val") + F.col("val_percent_rank")).alias("currency_avg_val_plus_val_percent_rank"),
        F.dayofweek("effective_from").alias("effective_from_dayofweek"),
    )
)

valid_price_tiers = (
    valid
    .filter(F.col("effective_from") >= F.lit("2024-05-28").cast("date"))
    .select(
        "effective_from",
        "currency",
        "val_percent_rank",
        "val_scaled_percent_rank",
        "effective_from_dayofweek",
        (F.col("val_scaled_percent_rank") * F.col("val_percent_rank")).alias("val_scaled_percent_rank_times_val_percent_rank"),
        F.coalesce("currency_avg_val_plus_val_percent_rank", F.lit(0)).alias("currency_avg_val_plus_val_percent_rank_filled"),
    )
)

kept_keys = (
    valid_price_tiers
    .select("currency")
    .intersect(
        df_price_tiers
        .filter(~F.col("currency").isin(["EUR", "THB"]))
        .select("currency")
    )
)

kept_rows = (
    valid_price_tiers
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.currency") == F.col("k.currency")), "inner")
    .select("s.*")
)

grouped = (
    kept_rows
    .groupBy(F.date_trunc("QUARTER", "effective_from").alias("quarter_start"))
    .agg(
        F.percentile_approx("val_percent_rank", [0.25, 0.5, 0.75]).alias("val_percent_rank_quartiles"),
        F.sum(F.when(F.col("currency") == "EUR", F.col("currency_avg_val_plus_val_percent_rank_filled")).otherwise(0)).alias("eur_currency_avg_val_plus_val_percent_rank_filled"),
        F.percentile_approx("val_percent_rank", 0.9).alias("val_percent_rank_p90"),
    )
    .filter(F.col("val_percent_rank_p90") >= 0)
)

result = (
    grouped
    .select(
        "quarter_start",
        "val_percent_rank_quartiles",
        "eur_currency_avg_val_plus_val_percent_rank_filled",
        "val_percent_rank_p90",
    )
)
