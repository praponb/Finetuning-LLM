from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("currency").orderBy(F.col("val").asc_nulls_last())
w_running = Window.partitionBy("currency")
w3 = Window.orderBy(F.col("currency_min_val").desc_nulls_last())
w4 = Window.partitionBy("currency").orderBy(F.col("val").desc())

lines = df_price_tiers.select("id", "currency", F.explode("tier_prices").alias("val"))

enriched = (
    lines
    .filter(~F.col("currency").isin(["THB", "USD"]))
    .select(
        "currency",
        "val",
        F.rank().over(w_part).alias("val_rank"),
        F.sum("val").over(w_running).alias("currency_sum_val"),
        F.min("val").over(w_running).alias("currency_min_val"),
    )
)

scored = (
    enriched
    .select(
        "currency",
        "val",
        "val_rank",
        F.rank().over(w3).alias("currency_min_val_rank"),
        F.count("currency_sum_val").over(w_running).alias("currency_count_currency_sum_val"),
        F.stddev_samp("val").over(w_running).alias("currency_stddev_samp_val"),
        F.dense_rank().over(w4).alias("val_dense_rank"),
    )
)

kept_keys = (
    scored
    .select("currency")
    .intersect(
        df_price_tiers
        .filter(F.col("effective_from") < "2024-07-01")
        .select("currency")
    )
)

kept_rows = (
    scored
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.currency") == F.col("k.currency")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .withColumn("currency_count_currency_sum_val_tier", F.when(F.col("currency_count_currency_sum_val") >= 13, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["currency", "currency_count_currency_sum_val_tier"], ["currency"]],
        "currency",
        "currency_count_currency_sum_val_tier",
    )
    .agg(
        F.max("currency_min_val_rank").alias("max_currency_min_val_rank"),
        F.countDistinct("currency").alias("distinct_currencies"),
        F.countDistinct("currency").alias("distinct_currencies_2"),
        F.max("currency_count_currency_sum_val").alias("max_currency_count_currency_sum_val"),
        F.count("*").alias("num_rows"),
        F.avg(F.when(F.col("currency") == "USD", F.col("val_rank"))).alias("usd_avg_val_rank"),
        F.grouping("currency").alias("gid"),
    )
    .orderBy(F.col("currency_count_currency_sum_val_tier").desc_nulls_first())
)
