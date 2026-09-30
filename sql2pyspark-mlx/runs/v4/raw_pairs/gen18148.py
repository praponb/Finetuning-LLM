from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy(F.col("val_adjusted").desc())
w_part = Window.partitionBy("currency")

flattened = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode_outer("tier_limits").alias("val"),
    )
)

base = (
    flattened
    .filter(F.col("val") <= 22)
    .select(
        "*",
        F.round((F.col("val") * 0.85), 2).alias("val_adjusted"),
        F.last_day("effective_from").alias("effective_from_month_end"),
    )
)

enriched = (
    base
    .withColumn("val_adjusted_cume_dist", F.cume_dist().over(w))
    .withColumn("currency_stddev_samp_val", F.stddev_samp("val").over(w_part))
    .withColumn("currency_sum_val", F.sum("val").over(w_part))
)

cleaned = (
    enriched
    .filter((F.col("effective_from") > F.lit("2024-09-25").cast("date")) & (F.col("val") >= 63))
    .select(
        "id",
        "effective_from",
        "val",
        "effective_from_month_end",
        "val_adjusted_cume_dist",
        "currency_stddev_samp_val",
        "currency_sum_val",
        F.initcap(F.trim("currency")).alias("currency_title"),
    )
)

result = (
    cleaned
    .groupBy(
        F.date_trunc("year", "effective_from_month_end").alias("year_start"),
        F.date_trunc("month", "effective_from").alias("activity_month"),
    )
    .agg(
        F.percentile_approx("val", 0.5).alias("val_p50"),
        F.round(F.avg("val_adjusted_cume_dist"), 2).alias("avg_val_adjusted_cume_dist"),
        F.percentile_approx("val", [0.25, 0.5, 0.75]).alias("val_quartiles"),
    )
    .filter(F.col("val_p50") < 0)
)
