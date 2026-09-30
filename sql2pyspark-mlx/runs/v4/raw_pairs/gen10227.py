from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code")
w_grp = Window.partitionBy("channel").orderBy(F.col("discount_pct").desc(), "id")
w3 = Window.partitionBy("channel").orderBy("starts_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("promo_code").orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_promotions
    .filter(F.col("discount_pct") >= 0.28)
    .withColumn("starts_on_plus_42d", F.date_add("starts_on", 42))
)

metrics = (
    filtered
    .select(
        "*",
        F.round((F.col("discount_pct") / F.sum("discount_pct").over(w)), 4).alias("discount_pct_share"),
        F.ntile(5).over(w_grp).alias("discount_pct_bucket"),
    )
)

scored = (
    metrics
    .withColumn("cumulative_discount_pct", F.sum("discount_pct").over(w3))
    .withColumn("first_known_discount_pct_bucket", F.first("discount_pct_bucket", ignorenulls=True).over(w4))
)

with_window = (
    scored
    .alias("e")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("e.channel"), F.lit("web")) == F.col("pr.channel")) & (F.col("e.ends_on") >= F.col("pr.starts_on")) & (F.col("e.ends_on") <= F.date_add(F.col("pr.ends_on"), 10)), "left")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

result = (
    with_window
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        "starts_on_plus_42d",
        "discount_pct_share",
        "discount_pct_bucket",
        F.when(F.col("channel").rlike("^(mobile|web)$"), "GROUP_B").when(F.col("channel").rlike("^(store|partner)$"), "PRIORITY").otherwise("OTHER_ZONE").alias("channel_zone"),
    )
)
