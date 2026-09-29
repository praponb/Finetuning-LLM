from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("currency")
w2 = Window.partitionBy("currency").orderBy(F.col("val").desc_nulls_last())
w3 = Window.partitionBy("currency").orderBy(F.col("val_percent_rank").desc())

lines = (
    df_price_tiers
    .select("id", "effective_from", "currency", F.explode("tier_prices").alias("val"))
)

with_windows = (
    lines
    .select(
        "*",
        F.min("val").over(w_running).alias("currency_min_val"),
        F.percent_rank().over(w2).alias("val_percent_rank"),
        F.avg("val").over(w_running).alias("currency_avg_val"),
    )
)

base = (
    with_windows
    .filter(F.col("val") > 52)
    .withColumn("val_int", F.col("val").cast("int"))
)

metrics = (
    base
    .select(
        "id",
        "effective_from",
        "val",
        "currency_min_val",
        "val_percent_rank",
        "currency_avg_val",
        "val_int",
        F.cume_dist().over(w3).alias("val_percent_rank_cume_dist"),
    )
)

result = (
    metrics
    .groupBy(
        F.date_trunc("MONTH", "effective_from").alias("activity_month"),
        F.when(F.col("val_percent_rank_cume_dist") >= 0.69, "PREMIUM").otherwise("STANDARD").alias("val_percent_rank_cume_dist_tier"),
    )
    .agg(
        F.round(F.avg("val_int"), 2).alias("avg_val_int"),
        F.countDistinct("id").alias("distinct_ids"),
        F.min("val").alias("min_val"),
        F.round(F.avg("currency_min_val"), 2).alias("avg_currency_min_val"),
        F.min("currency_min_val").alias("min_currency_min_val"),
        F.percentile_approx("val_percent_rank", [0.25, 0.5, 0.75]).alias("val_percent_rank_quartiles"),
    )
)
