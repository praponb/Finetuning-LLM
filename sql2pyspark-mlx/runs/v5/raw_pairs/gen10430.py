from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code", "channel").orderBy(F.col("starts_on_month").desc(), "id")

valid = (
    df_promotions
    .filter((F.col("promo_code") == "SPRING10") & (F.col("starts_on") > F.lit("2023-08-15").cast("date")))
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        F.month("starts_on").alias("starts_on_month"),
        F.date_format("starts_on", "MMM yyyy").alias("starts_on_label"),
        F.coalesce("channel", F.lit("NONE")).alias("resolved_channel"),
    )
)

deduped = (
    valid
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

remaining_keys = (
    deduped
    .select("channel", "id")
    .intersectAll(
        df_promotions
        .filter(F.col("ends_on") > "2025-02-25")
        .select("channel", "id")
    )
)

verified = (
    deduped
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.channel") == F.col("k.channel")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

filtered = (
    verified
    .filter((F.col("starts_on") > F.lit("2023-06-29").cast("date")) & F.col("channel").isNotNull())
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        "starts_on_label",
        "resolved_channel",
        "row_num",
        F.length("channel").alias("channel_length"),
        F.coalesce("channel", F.lit("OTHER")).alias("resolved_channel_2"),
    )
)

result = (
    filtered
    .select(
        "id",
        "channel",
        "promo_code",
        "discount_pct",
        "resolved_channel",
        "row_num",
        "channel_length",
        "resolved_channel_2",
    )
    .orderBy(F.col("channel_length").asc_nulls_first())
)
