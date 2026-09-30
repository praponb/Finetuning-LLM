from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("total_discount_pct").desc())

base = (
    df_promotions
    .filter(F.col("ends_on") >= F.lit("2023-06-23").cast("date"))
    .select(
        "id",
        "channel",
        "promo_code",
        "ends_on",
        "discount_pct",
        F.col("discount_pct").cast("int").alias("discount_pct_int"),
    )
)

remaining_keys = (
    base
    .select("promo_code")
    .intersectAll(
        df_promotions
        .filter(F.col("discount_pct") >= 0.14)
        .select("promo_code")
    )
)

reconciled = (
    base
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.promo_code") == F.col("k.promo_code")), "inner")
    .select("s.*")
)

cleaned = (
    reconciled
    .filter((F.col("ends_on") >= F.lit("2023-06-13").cast("date")) & (F.col("discount_pct_int") == 50))
    .select(
        "*",
        F.upper("promo_code").alias("promo_code_upper"),
        F.coalesce("channel", F.lit("OTHER")).alias("resolved_channel"),
    )
)

filtered = (
    cleaned
    .filter(F.col("discount_pct_int").between(47, 59))
    .withColumn("channel_upper", F.upper("channel"))
    .withColumn("channel_filled", F.coalesce("channel", F.lit("N/A")))
)

kept_keys = (
    filtered
    .select("channel", "promo_code")
    .intersectAll(
        df_promotions
        .filter(F.col("starts_on") < "2025-07-01")
        .select("channel", "promo_code")
    )
)

reconciled_promotions = (
    filtered
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.channel") == F.col("k.channel")) & (F.col("s.promo_code") == F.col("k.promo_code")), "inner")
    .select("s.*")
)

result = (
    reconciled_promotions
    .groupBy(
        "channel",
        F.date_trunc("YEAR", "ends_on").alias("year_start"),
        F.when(F.col("discount_pct") >= 0.28, "HIGH").otherwise("LOW").alias("discount_pct_tier"),
    )
    .agg(
        F.sum(F.when(F.col("promo_code") == "SPRING10", F.col("discount_pct_int")).otherwise(0)).alias("spring10_discount_pct_int"),
        F.max("discount_pct").alias("max_discount_pct"),
        F.sum("discount_pct").alias("total_discount_pct"),
    )
    .withColumn("total_discount_pct_rank", F.rank().over(w))
    .orderBy(F.col("discount_pct_tier").desc_nulls_last())
)
