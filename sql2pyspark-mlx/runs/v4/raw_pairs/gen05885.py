from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promotion_promo_code").orderBy("promo_id", "id", "promo_id", "promo_id_2")
w2 = Window.partitionBy("promotion_promo_code_2").orderBy(F.col("promo_id_2").desc(), "id", "promo_id", "promo_id_2")
w3 = Window.orderBy(F.col("promo_id_2").asc_nulls_first())
w4 = Window.partitionBy("channel").orderBy("ends_on", "id", "promo_id", "promo_id_2")

valid = (
    df_promotions
    .filter(F.col("ends_on") > "2023-09-01")
    .select("*", F.substring("promo_code", 1, 2).alias("promo_code_prefix"))
)

with_window = (
    valid
    .alias("a")
    .join(df_promotions.alias("pr"), (F.col("a.channel").eqNullSafe(F.col("pr.channel"))) & F.col("a.starts_on").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "inner")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

matched = (
    with_window
    .alias("a")
    .join(df_promotions.alias("pr"), (F.col("a.channel").eqNullSafe(F.col("pr.channel"))) & F.col("a.ends_on").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "inner")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id_2"),
        F.col("pr.promo_code").alias("promotion_promo_code_2"),
        F.col("pr.discount_pct").alias("promotion_discount_pct_2"),
    )
)

metrics = (
    matched
    .select(
        "*",
        F.ntile(5).over(w).alias("promo_id_bucket"),
        F.row_number().over(w2).alias("event_seq"),
        F.rank().over(w3).alias("promo_id_2_rank"),
        F.lag("discount_pct", 2, 0).over(w4).alias("prev_discount_pct"),
    )
)

result = (
    metrics
    .select(
        "id",
        "promo_id",
        "promo_id_2",
        "promotion_promo_code",
        "discount_pct",
        "promotion_discount_pct_2",
        "prev_discount_pct",
    )
    .unpivot(
        ["id", "promo_id", "promo_id_2", "promotion_promo_code"],
        ["discount_pct", "promotion_discount_pct_2", "prev_discount_pct"],
        "metric",
        "metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
