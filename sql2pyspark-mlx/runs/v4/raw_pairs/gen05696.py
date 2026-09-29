from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code")
w2 = Window.orderBy("discount_pct")
w3 = Window.partitionBy("promotion_promo_code")
w4 = Window.orderBy("ends_on", "id", "promo_id", "promo_id_2").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("promo_code").orderBy("promo_id_2", "id", "promo_id", "promo_id_2")
w6 = Window.partitionBy("promotion_promo_code_3")
w7 = Window.partitionBy("promotion_promo_code_3", "promotion_promo_code_4").orderBy("promo_id_share", "id", "promo_id", "promo_id_2", "promo_id_3", "promo_id_4").rowsBetween(Window.unboundedPreceding, Window.currentRow)

matched = (
    df_promotions
    .alias("a")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("a.channel"), F.lit("web")) == F.col("pr.channel")) & F.col("a.ends_on").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "inner")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

matched_promotions = (
    matched
    .alias("src")
    .join(df_promotions.alias("pr"), (F.col("src.channel") == F.col("pr.channel")) & (F.col("src.ends_on") >= F.col("pr.starts_on")) & (F.col("src.ends_on") <= F.date_add(F.col("pr.ends_on"), 7)), "left")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id_2"),
        F.col("pr.promo_code").alias("promotion_promo_code_2"),
        F.col("pr.discount_pct").alias("promotion_discount_pct_2"),
    )
)

with_windows = (
    matched_promotions
    .withColumn("promo_id_share", F.round((F.col("promo_id") / F.sum("promo_id").over(w)), 4))
    .withColumn("discount_pct_percent_rank", F.percent_rank().over(w2))
    .withColumn("promotion_promo_code_sum_promotion_discount_pct", F.sum("promotion_discount_pct").over(w3))
    .withColumn("running_promotion_discount_pct", F.sum("promotion_discount_pct").over(w4))
    .withColumn("promo_id_2_bucket", F.ntile(5).over(w5))
)

base = (
    with_windows
    .filter(F.col("promotion_promo_code").isNotNull())
    .withColumn("least_running_promotion_discount_pct_promo_id_share", F.least("running_promotion_discount_pct", "promo_id_share"))
)

attributed = (
    base
    .alias("s")
    .join(df_promotions.alias("pr"), (F.col("s.channel").eqNullSafe(F.col("pr.channel"))) & (F.col("s.ends_on") >= F.col("pr.starts_on")) & (F.col("s.ends_on") <= F.date_add(F.col("pr.ends_on"), 10)), "left")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id_3"),
        F.col("pr.promo_code").alias("promotion_promo_code_3"),
        F.col("pr.discount_pct").alias("promotion_discount_pct_3"),
    )
)

matched_promotions_2 = (
    attributed
    .alias("e")
    .join(df_promotions.alias("pr"), (F.col("e.channel").eqNullSafe(F.col("pr.channel"))) & (F.col("e.starts_on") >= F.col("pr.starts_on")) & (F.col("e.starts_on") <= F.date_add(F.col("pr.ends_on"), 3)), "inner")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id_4"),
        F.col("pr.promo_code").alias("promotion_promo_code_4"),
        F.col("pr.discount_pct").alias("promotion_discount_pct_4"),
    )
)

scored = (
    matched_promotions_2
    .withColumn("promotion_promo_code_3_max_promo_id_2", F.max("promo_id_2").over(w6))
    .withColumn("first_known_promo_id_4", F.first("promo_id_4", ignorenulls=True).over(w7))
)

result = (
    scored
    .select(
        "id",
        "promo_id",
        "promo_id_2",
        "promo_id_3",
        "promo_id_4",
        "promotion_promo_code_3",
        F.col("discount_pct_percent_rank").alias("DISCOUNT_PCT_PERCENT_RANK"),
        F.col("promotion_discount_pct_2").alias("PROMOTION_DISCOUNT_PCT_2_VAL"),
    )
    .unpivot(
        ["id", "promo_id", "promo_id_2", "promo_id_3", "promo_id_4", "promotion_promo_code_3"],
        ["DISCOUNT_PCT_PERCENT_RANK", "PROMOTION_DISCOUNT_PCT_2_VAL"],
        "metric",
        "reading",
    )
)
