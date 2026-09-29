from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("starts_on").desc())
w2 = Window.partitionBy("promo_code").orderBy("rank_in_group", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("promo_code").orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("channel").orderBy("ends_on_year", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("first_known_promo_code_length", "id")
w6 = Window.partitionBy("channel", "promo_code").orderBy("first_known_ends_on_year", "id")
w7 = Window.partitionBy("promo_code", "channel").orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("channel").orderBy(F.col("ends_on_year").desc_nulls_last())
w9 = Window.partitionBy("channel")

cleaned = (
    df_promotions
    .filter((F.col("starts_on") > F.to_date(F.lit("2023-08-23"))) & F.col("ends_on").between("2023-07-01", "2023-11-01"))
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "discount_pct",
        F.year("ends_on").alias("ends_on_year"),
        F.length("promo_code").alias("promo_code_length"),
        F.coalesce("channel", F.lit("UNKNOWN")).alias("channel_clean"),
    )
)

deduped = (
    cleaned
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
)

with_windows = (
    deduped
    .withColumn("first_known_ends_on_year", F.first("ends_on_year", ignorenulls=True).over(w2))
    .withColumn("first_known_promo_code_length", F.first("promo_code_length", ignorenulls=True).over(w3))
    .withColumn("cumulative_ends_on_year", F.sum("ends_on_year").over(w4))
)

enriched = (
    with_windows
    .select(
        "*",
        F.lead("discount_pct", 1).over(w5).alias("next_discount_pct"),
        F.ntile(4).over(w6).alias("first_known_ends_on_year_bucket"),
        F.last("channel", ignorenulls=True).over(w7).alias("last_known_channel"),
    )
)

with_windows_promotions = (
    enriched
    .filter(F.col("next_discount_pct") >= 0.2)
    .select(
        "id",
        "channel",
        "starts_on",
        "ends_on_year",
        "channel_clean",
        "first_known_ends_on_year",
        "first_known_promo_code_length",
        F.rank().over(w8).alias("ends_on_year_rank"),
        F.round((F.col("first_known_ends_on_year_bucket") / F.sum("first_known_ends_on_year_bucket").over(w9)), 4).alias("first_known_ends_on_year_bucket_share"),
    )
)

result = (
    with_windows_promotions
    .groupBy(
        F.when(F.col("first_known_ends_on_year") >= 73, "HIGH").otherwise("LOW").alias("first_known_ends_on_year_tier"),
    )
    .agg(
        F.count_if(F.col("channel") == "partner").alias("partner_events"),
        F.count_if(F.col("channel") == "web").alias("web_events"),
    )
    .filter(F.col("partner_events") > 1)
)
