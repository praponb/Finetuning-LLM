from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("val").asc_nulls_first())
w2 = Window.orderBy(F.col("val_cume_dist").desc())
w_part = Window.partitionBy("currency").orderBy("val_cume_dist")

exploded = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("tier_prices").alias("val"),
    )
)

kept_keys = (
    exploded
    .select("id", "plan")
    .subtract(
        df_price_tiers
        .filter(F.col("effective_from") > "2024-11-01")
        .select("id", "plan")
    )
)

verified = (
    exploded
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.plan") == F.col("k.plan")), "inner")
    .select("s.*")
)

enriched = (
    verified
    .filter(F.col("plan") == "basic")
    .select("effective_from", "currency", F.cume_dist().over(w).alias("val_cume_dist"))
)

metrics = (
    enriched
    .select(
        "effective_from",
        "currency",
        F.rank().over(w2).alias("val_cume_dist_rank"),
        F.percent_rank().over(w_part).alias("val_cume_dist_percent_rank"),
        F.dense_rank().over(w_part).alias("val_cume_dist_dense_rank"),
    )
)

result = (
    metrics
    .groupBy(
        F.when(F.col("val_cume_dist_dense_rank") >= 9, "LARGE").otherwise("SMALL").alias("val_cume_dist_dense_rank_tier"),
        F.date_trunc("QUARTER", "effective_from").alias("quarter_start"),
        F.date_trunc("YEAR", "effective_from").alias("year_start"),
    )
    .agg(
        F.round(F.avg("val_cume_dist_dense_rank"), 2).alias("avg_val_cume_dist_dense_rank"),
        F.count_if(F.col("currency") == "USD").alias("usd_events"),
        F.percentile_approx("val_cume_dist_percent_rank", [0.25, 0.5, 0.75]).alias("val_cume_dist_percent_rank_quartiles"),
    )
    .filter(F.col("avg_val_cume_dist_dense_rank") >= 0)
    .orderBy(F.col("avg_val_cume_dist_dense_rank").desc_nulls_first())
)
