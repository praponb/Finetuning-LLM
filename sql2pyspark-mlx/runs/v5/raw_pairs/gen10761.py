from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel", "promotion_promo_code").orderBy(F.col("discount_pct_whole").desc())
w_part = Window.partitionBy("channel").orderBy("discount_pct", "id", "promo_id").rowsBetween(-3, Window.currentRow)

base = (
    df_promotions
    .filter((F.col("starts_on") >= F.lit("2023-07-08").cast("date")) & F.col("channel").isNotNull())
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "discount_pct",
        F.floor("discount_pct").alias("discount_pct_whole"),
    )
)

matched = (
    base
    .alias("a")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("a.channel"), F.lit("web")) == F.col("pr.channel")) & (F.col("a.starts_on") >= F.col("pr.starts_on")) & (F.col("a.starts_on") <= F.date_add(F.col("pr.ends_on"), 10)), "left")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

deduped = (
    matched
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

metrics = (
    deduped
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "discount_pct",
        "discount_pct_whole",
        "promo_id",
        "promotion_promo_code",
        "promotion_discount_pct",
        F.sum("discount_pct_whole").over(w_part).alias("last4_sum_discount_pct_whole"),
        F.max("discount_pct_whole").over(w_part).alias("last4_max_discount_pct_whole"),
        F.avg("discount_pct_whole").over(w_part).alias("last4_avg_discount_pct_whole"),
    )
)

filtered = (
    metrics
    .filter(F.col("channel") == "mobile")
    .select(
        "*",
        F.when(F.col("promo_id") >= 10, "High").when(F.col("promo_id") >= 5, "Medium").otherwise("Low").alias("promo_id_band"),
    )
)

base_promotions = (
    filtered
    .filter((F.col("starts_on") > F.lit("2023-10-04").cast("date")) & F.col("starts_on").between("2023-03-06", "2025-06-01"))
    .select(
        "*",
        F.coalesce("discount_pct_whole", F.lit(0)).alias("discount_pct_whole_filled"),
        F.coalesce("channel", F.lit("DIRECT")).alias("resolved_channel"),
    )
)

by_type = (
    base_promotions
    .groupBy("channel")
    .pivot("promo_code", ["SPRING10", "VIP", "BUNDLE"])
    .agg(F.max("last4_sum_discount_pct_whole"))
    .select(
        "channel",
        F.col("SPRING10").alias("spring10"),
        F.col("VIP").alias("vip"),
        F.col("BUNDLE").alias("bundle"),
    )
)

result = (
    by_type
    .select("channel", "spring10", "bundle")
    .orderBy("bundle", "spring10")
)
