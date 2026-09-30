from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy("ends_on_plus_54d", "id")
w2 = Window.partitionBy("promo_code").orderBy("starts_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("promo_code").orderBy(F.col("ends_on_plus_54d").desc_nulls_first(), "id")
w4 = Window.partitionBy("channel", "promo_code").orderBy(F.col("starts_on").desc())

cleaned = (
    df_promotions
    .filter(~F.col("channel").eqNullSafe(F.lit("mobile")))
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        F.date_format("starts_on", "yyyy-MM-dd").alias("starts_on_label"),
        F.when(F.col("discount_pct") >= 0.29, "High").when(F.col("discount_pct") >= 0.1, "Medium").otherwise("Low").alias("discount_pct_band"),
        F.coalesce("channel", F.lit("UNKNOWN")).alias("channel_clean"),
    )
)

cleaned_promotions = (
    cleaned
    .filter(F.col("channel").eqNullSafe(F.lit("partner")))
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        "starts_on_label",
        "discount_pct_band",
        F.date_add("ends_on", 54).alias("ends_on_plus_54d"),
        F.length("promo_code").alias("promo_code_length"),
    )
)

scored = (
    cleaned_promotions
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        "starts_on_label",
        "discount_pct_band",
        "ends_on_plus_54d",
        F.row_number().over(w).alias("row_seq"),
        F.first("channel", ignorenulls=True).over(w2).alias("first_known_channel"),
    )
)

deduped = (
    scored
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
)

deduped_promotions = (
    deduped
    .withColumn("rn", F.rank().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "id",
        "channel",
        "promo_code",
        "discount_pct",
        "discount_pct_band",
        "ends_on_plus_54d",
        "dedup_rank",
    )
)

result = (
    deduped_promotions
    .select(
        "id",
        "channel",
        "promo_code",
        "discount_pct",
        "discount_pct_band",
        "dedup_rank",
        F.when(F.col("promo_code").rlike("^(VIP|BUNDLE)$"), "EXTENDED").otherwise("CORE").alias("promo_code_zone"),
    )
    .orderBy(F.col("promo_code").desc_nulls_last(), F.col("dedup_rank").desc())
)
