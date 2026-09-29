from pyspark.sql import functions as F

kept_keys = (
    df_promotions
    .select("promo_code", "id")
    .intersect(
        df_promotions
        .filter(F.col("promo_code") != "BUNDLE")
        .select("promo_code", "id")
    )
)

reconciled = (
    df_promotions
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.promo_code") == F.col("k.promo_code")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .rollup("promo_code", F.date_trunc("YEAR", "ends_on").alias("year_start"))
    .agg(
        F.count(F.when(F.col("promo_code") == "BUNDLE", 1)).alias("bundle_count"),
        F.size(F.collect_set("channel")).alias("num_channels"),
    )
    .filter(F.col("num_channels") > 1)
    .orderBy(F.col("num_channels").desc())
)
