from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy(F.col("starts_on").desc(), "id")
w2 = Window.partitionBy("channel").orderBy(F.col("ends_on_month").asc_nulls_last())
w3 = Window.partitionBy("promo_code").orderBy("discount_pct", "id")

base = (
    df_promotions
    .filter((F.col("starts_on") >= F.lit("2023-05-17").cast("date")) & (~F.col("channel").eqNullSafe(F.lit("mobile"))))
    .withColumn("channel_group", F.when(F.col("channel").isin(["mobile", "web"]), "Priority").otherwise("Other"))
    .withColumn("ends_on_month", F.month("ends_on"))
)

latest = (
    base
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

enriched = (
    latest
    .select(
        "id",
        "promo_code",
        "discount_pct",
        "channel_group",
        "ends_on_month",
        "rank_in_group",
        F.percent_rank().over(w2).alias("ends_on_month_percent_rank"),
        F.row_number().over(w3).alias("seq_no"),
    )
)

cleaned = (
    enriched
    .filter(F.col("rank_in_group") >= 1)
    .select(
        "id",
        "promo_code",
        "discount_pct",
        "rank_in_group",
        "seq_no",
        F.upper("promo_code").alias("promo_code_upper"),
        F.round("discount_pct", 2).alias("discount_pct_rounded"),
    )
)

matched_keys = (
    cleaned
    .select("promo_code")
    .exceptAll(
        df_promotions
        .filter(F.col("discount_pct") >= 0.17)
        .select("promo_code")
    )
)

verified = (
    cleaned
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.promo_code") == F.col("k.promo_code")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy("seq_no")
    .agg(
        F.max(F.when(F.col("promo_code") == "BUNDLE", F.col("discount_pct"))).alias("bundle_max_discount_pct"),
        F.min("seq_no").alias("min_seq_no"),
        F.count_if(F.col("promo_code") == "SPRING10").alias("spring10_events"),
        F.round(F.avg("discount_pct_rounded"), 2).alias("avg_discount_pct_rounded"),
    )
    .filter(F.col("avg_discount_pct_rounded") > 0)
    .orderBy(F.col("avg_discount_pct_rounded").asc_nulls_last())
)
