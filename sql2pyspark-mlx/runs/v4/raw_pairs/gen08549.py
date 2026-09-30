from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("discount_pct_adjusted").desc_nulls_last())
w3 = Window.partitionBy("channel").orderBy("starts_on", "id")
w4 = Window.partitionBy("channel").orderBy(F.col("discount_pct_adjusted").desc(), "id", "promo_id", "promo_id_2")
w5 = Window.partitionBy("channel")

base = (
    df_promotions
    .filter((F.col("ends_on") > F.lit("2024-06-02").cast("date")) & F.col("channel").isNotNull() & F.col("channel").isin(["mobile", "partner", "store"]))
    .select(
        "id",
        "channel",
        "starts_on",
        "ends_on",
        "discount_pct",
        F.round((F.col("discount_pct") * 0.85), 2).alias("discount_pct_adjusted"),
        F.last_day("ends_on").alias("ends_on_month_end"),
    )
)

enriched = (
    base
    .withColumn("last_known_discount_pct_adjusted", F.last("discount_pct_adjusted", ignorenulls=True).over(w))
    .withColumn("discount_pct_adjusted_dense_rank", F.dense_rank().over(w2))
    .withColumn("prev_discount_pct_adjusted", F.lag("discount_pct_adjusted", 2).over(w3))
)

with_window = (
    enriched
    .alias("e")
    .join(df_promotions.alias("pr"), (F.col("e.channel") == F.col("pr.channel")) & (F.col("e.starts_on") >= F.col("pr.starts_on")) & (F.col("e.starts_on") < F.col("pr.ends_on") + F.expr("INTERVAL 7 DAYS")), "inner")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

with_window_promotions = (
    with_window
    .alias("a")
    .join(df_promotions.alias("pr"), (F.col("a.channel").eqNullSafe(F.col("pr.channel"))) & F.col("a.ends_on").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "left")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id_2"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct_2"),
    )
)

result = (
    with_window_promotions
    .withColumn("seq_no", F.row_number().over(w4))
    .withColumn("channel_avg_discount_pct", F.avg("discount_pct").over(w5))
)
