from pyspark.sql import functions as F

kept_keys = (
    df_promotions
    .select("channel")
    .subtract(
        df_promotions
        .filter(F.col("channel") == "partner")
        .select("channel")
    )
)

reconciled = (
    df_promotions
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.channel") == F.col("k.channel")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select(
        "id",
        "channel",
        "promo_code",
        "ends_on",
        "discount_pct",
        F.when(F.col("promo_code").rlike("^(SPRING10|WELCOME5)$"), "PRIORITY").otherwise("GROUP_B").alias("promo_code_zone"),
    )
)
