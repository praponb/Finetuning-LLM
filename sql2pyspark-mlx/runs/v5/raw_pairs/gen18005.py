from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("discount_pct").desc(), "id")
w2 = Window.orderBy("starts_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("channel").orderBy(F.col("cumulative_discount_pct").desc_nulls_first(), "id")
w4 = Window.orderBy("discount_pct", "id", "promo_id")
w5 = Window.partitionBy("promotion_promo_code").orderBy("discount_pct", "id", "promo_id")
w6 = Window.partitionBy("channel").orderBy("discount_pct_bucket", "id", "promo_id")

with_windows = (
    df_promotions
    .filter(F.col("discount_pct") <= 0.27)
    .withColumn("discount_pct_bucket", F.ntile(10).over(w))
    .withColumn("cumulative_discount_pct", F.sum("discount_pct").over(w2))
)

deduped = (
    with_windows
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
)

attributed = (
    deduped
    .alias("src")
    .join(df_promotions.alias("pr"), (F.col("src.channel") == F.col("pr.channel")) & (F.col("src.ends_on") >= F.col("pr.starts_on")) & (F.col("src.ends_on") <= F.date_add(F.col("pr.ends_on"), 3)), "left")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

with_windows_promotions = (
    attributed
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        "discount_pct_bucket",
        "promo_id",
        "promotion_promo_code",
        "promotion_discount_pct",
        F.ntile(10).over(w4).alias("discount_pct_bucket_2"),
        F.lag("discount_pct", 1).over(w5).alias("prev_discount_pct"),
    )
)

filtered = (
    with_windows_promotions
    .filter(F.col("ends_on") > F.lit("2023-07-02").cast("date"))
    .withColumn("prev_discount_pct_per_promo_id", F.round((F.col("prev_discount_pct") / F.col("promo_id")), 2))
    .withColumn("discount_pct_plus_discount_pct_bucket_2", (F.col("discount_pct") + F.col("discount_pct_bucket_2")))
)

ranked = (
    filtered
    .withColumn("rank_in_group", F.row_number().over(w6))
    .filter(F.col("rank_in_group") == 1)
    .select(
        "id",
        "channel",
        "starts_on",
        "ends_on",
        "discount_pct",
        "discount_pct_bucket",
        "promo_id",
        "promotion_discount_pct",
        "discount_pct_bucket_2",
    )
)

attributed_promotions = (
    ranked
    .alias("s")
    .join(df_promotions.alias("pr"), (F.col("s.channel") == F.col("pr.channel")) & (F.col("s.ends_on") >= F.col("pr.starts_on")) & (F.col("s.ends_on") <= F.date_add(F.col("pr.ends_on"), 3)), "left")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id_2"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct_2"),
    )
)

result = (
    attributed_promotions
    .groupBy(F.date_trunc("year", "ends_on").alias("year_start"))
    .agg(
        F.count("*").alias("row_count"),
        F.count_if(F.col("channel") == "partner").alias("partner_events"),
        F.count_if(F.col("promo_code") == "VIP").alias("vip_events"),
        F.sum("promo_id_2").alias("total_promo_id_2"),
        F.max("promotion_discount_pct_2").alias("max_promotion_discount_pct_2"),
    )
    .filter(F.col("vip_events") > 1)
)
