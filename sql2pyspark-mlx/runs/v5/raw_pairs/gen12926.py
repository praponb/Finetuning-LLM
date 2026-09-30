from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("discount_pct_tier").orderBy(F.col("store_avg_discount_pct").desc())

matched_keys = (
    df_promotions
    .select("id", "promo_code")
    .subtract(
        df_promotions
        .filter(F.year("starts_on") == 2023)
        .select("id", "promo_code")
    )
)

kept_rows = (
    df_promotions
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.promo_code") == F.col("k.promo_code")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy(
        F.when(F.col("discount_pct") >= 0.36, "PREMIUM").otherwise("STANDARD").alias("discount_pct_tier"),
        "channel",
    )
    .agg(
        F.percentile_approx("discount_pct", [0.25, 0.5, 0.75]).alias("discount_pct_quartiles"),
        F.avg(F.when(F.col("channel") == "store", F.col("discount_pct"))).alias("store_avg_discount_pct"),
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
    )
    .filter(F.col("store_avg_discount_pct") > 0)
    .withColumn("store_avg_discount_pct_dense_rank", F.dense_rank().over(w))
    .orderBy(F.col("channel").desc())
)
