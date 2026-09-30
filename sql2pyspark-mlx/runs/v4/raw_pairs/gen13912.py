from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code")
w2 = Window.partitionBy("channel")

matched_keys = (
    df_promotions
    .select("id", "promo_code")
    .intersectAll(
        df_promotions
        .filter(F.month("starts_on").isin([1, 5, 11]))
        .select("id", "promo_code")
    )
)

kept_rows = (
    df_promotions
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.promo_code") == F.col("k.promo_code")), "inner")
    .select("s.*")
)

kept_keys = (
    kept_rows
    .select("promo_code", "channel")
    .subtract(
        df_promotions
        .filter(F.col("promo_code") == "BUNDLE")
        .select("promo_code", "channel")
    )
)

kept_rows_promotions = (
    kept_rows
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.promo_code") == F.col("k.promo_code")) & (F.col("s.channel") == F.col("k.channel")), "inner")
    .select("s.*")
)

remaining_keys = (
    kept_rows_promotions
    .select("promo_code", "channel")
    .exceptAll(
        df_promotions
        .filter(F.col("channel") == "partner")
        .select("promo_code", "channel")
    )
)

reconciled = (
    kept_rows_promotions
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.promo_code") == F.col("k.promo_code")) & (F.col("s.channel") == F.col("k.channel")), "inner")
    .select("s.*")
)

with_windows = (
    reconciled
    .withColumn("discount_pct_share", F.round((F.col("discount_pct") / F.sum("discount_pct").over(w)), 4))
    .withColumn("discount_pct_share_2", F.round((F.col("discount_pct") / F.sum("discount_pct").over(w2)), 4))
)

result = (
    with_windows
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        "discount_pct_share",
        "discount_pct_share_2",
        F.when(F.col("promo_code").rlike("^(WELCOME5|BUNDLE)$"), "PRIORITY").otherwise("CORE").alias("promo_code_zone"),
    )
    .orderBy(F.col("discount_pct").desc_nulls_first(), F.col("promo_code_zone").desc())
)
