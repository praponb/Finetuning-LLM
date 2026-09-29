from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy("starts_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("discount_pct", "id").rowsBetween(-5, -1)
w3 = Window.orderBy(F.col("starts_on_dayofweek").asc_nulls_last())

filtered = (
    df_promotions
    .filter((F.col("ends_on") > F.to_date(F.lit("2023-06-15"))) & F.col("promo_code").isin(["WELCOME5", "SPRING10", "BUNDLE"]))
    .withColumn("ends_on_dayofweek", F.dayofweek("ends_on"))
    .withColumn("channel_clean", F.coalesce("channel", F.lit("UNKNOWN")))
)

with_windows = (
    filtered
    .select(
        "*",
        F.first("channel", ignorenulls=True).over(w).alias("first_known_channel"),
        F.avg("discount_pct").over(w2).alias("discount_pct_avg_5_prior"),
    )
)

base = (
    with_windows
    .filter((~F.col("channel").eqNullSafe(F.lit("partner"))) & (F.col("ends_on") >= F.lit("2023-06-27").cast("date")))
    .select(
        "id",
        "ends_on",
        "discount_pct",
        "ends_on_dayofweek",
        "channel_clean",
        "first_known_channel",
        "discount_pct_avg_5_prior",
        F.dayofweek("starts_on").alias("starts_on_dayofweek"),
        F.coalesce("channel", F.lit("NONE")).alias("resolved_channel"),
    )
)

remaining_keys = (
    base
    .select("id")
    .intersectAll(
        df_promotions
        .filter(F.col("ends_on") > "2023-04-01")
        .select("id")
    )
)

reconciled = (
    base
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

scored = (
    reconciled
    .filter(F.col("starts_on_dayofweek") == 40)
    .withColumn("starts_on_dayofweek_rank", F.rank().over(w3))
)

result = (
    scored
    .select(
        "id",
        "ends_on",
        "discount_pct",
        "ends_on_dayofweek",
        "first_known_channel",
        "starts_on_dayofweek",
        "resolved_channel",
    )
    .orderBy(F.col("id").desc_nulls_last())
)
