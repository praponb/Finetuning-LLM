from pyspark.sql import functions as F

remaining_keys = (
    df_promotions
    .select("id")
    .subtract(
        df_promotions
        .filter(F.col("promo_code").isin(["WELCOME5", "SPRING10"]))
        .select("id")
    )
)

kept_rows = (
    df_promotions
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select(
        "channel",
        "promo_code",
        "ends_on",
        F.when(F.col("promo_code").rlike("^(BUNDLE|SPRING10)$"), "GROUP_A").otherwise("EXTENDED").alias("promo_code_zone"),
    )
    .orderBy(F.col("channel").asc_nulls_last())
)
