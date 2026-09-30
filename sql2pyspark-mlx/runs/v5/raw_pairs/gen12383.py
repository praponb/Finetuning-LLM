from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promotion_promo_code", "promo_code").orderBy("promo_id", "id", "promo_id")
w2 = Window.partitionBy("promo_code").orderBy(F.col("promotion_discount_pct").desc(), "id", "promo_id")
w_recent = Window.partitionBy("channel").orderBy("promo_id", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("promo_code").orderBy("discount_pct_rounded", "id", "promo_id")
w5 = Window.partitionBy("promotion_promo_code", "promo_code")

cleaned = (
    df_promotions
    .filter(F.col("channel") == "store")
    .select(
        "*",
        F.add_months("ends_on", 6).alias("ends_on_plus_months"),
        F.round("discount_pct", 0).alias("discount_pct_rounded"),
    )
)

valid = (
    cleaned
    .filter(F.month("ends_on_plus_months").isin([1, 3, 10]))
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        "ends_on_plus_months",
        "discount_pct_rounded",
        F.when(F.col("channel").isin(["web", "partner"]), "Group A").otherwise("Other").alias("channel_group"),
    )
)

attributed = (
    valid
    .alias("src")
    .join(df_promotions.alias("pr"), (F.col("src.channel") == F.col("pr.channel")) & (F.col("src.starts_on") >= F.col("pr.starts_on")) & (F.col("src.starts_on") <= F.date_add(F.col("pr.ends_on"), 7)), "left")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

metrics = (
    attributed
    .filter(F.month("ends_on").isin([1, 2, 5]))
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "channel_group",
        "promo_id",
        "promotion_promo_code",
        "promotion_discount_pct",
        F.row_number().over(w).alias("row_seq"),
        F.ntile(4).over(w2).alias("promotion_discount_pct_bucket"),
        F.last("discount_pct_rounded", ignorenulls=True).over(w_recent).alias("last_known_discount_pct_rounded"),
        F.row_number().over(w4).alias("row_seq_2"),
        F.round((F.col("promo_id") / F.sum("promo_id").over(w5)), 4).alias("promo_id_share"),
    )
)

result = (
    metrics
    .groupBy(
        F.when(F.col("row_seq_2") >= 7, "LARGE").otherwise("SMALL").alias("row_seq_2_tier"),
    )
    .agg(
        F.sum(F.when(F.col("promotion_promo_code") == "SPRING10", F.col("promo_id")).otherwise(0)).alias("spring10_promo_id"),
        F.round(F.sum("promo_id") / F.nullif(F.count(F.when(F.col("promotion_promo_code") == "SPRING10", 1)), F.lit(0)), 2).alias("promo_id_per_spring10"),
        F.count("*").alias("num_rows"),
        F.avg(F.when(F.col("promotion_promo_code") == "SPRING10", F.col("promotion_discount_pct"))).alias("spring10_avg_promotion_discount_pct"),
        F.sum(F.when(F.col("promo_code") == "SPRING10", F.col("promo_id")).otherwise(0)).alias("spring10_promo_id_2"),
        F.size(F.collect_set("promo_code")).alias("num_promo_codes"),
    )
    .filter(F.col("num_promo_codes") > 1)
    .orderBy(F.col("spring10_avg_promotion_discount_pct").desc())
)
