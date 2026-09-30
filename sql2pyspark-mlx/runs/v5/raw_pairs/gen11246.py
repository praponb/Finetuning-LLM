from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("is_store", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("channel").orderBy("starts_on", "id")
w3 = Window.orderBy("starts_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("channel")
w5 = Window.orderBy(F.col("promo_id").desc_nulls_last())

cleaned = (
    df_promotions
    .filter((F.col("starts_on") >= F.lit("2023-08-02").cast("date")) & (F.col("ends_on") <= "2024-04-01"))
    .select(
        "id",
        "channel",
        "starts_on",
        "ends_on",
        F.when(F.col("channel") == "store", 1).otherwise(0).alias("is_store"),
    )
)

metrics = (
    cleaned
    .filter(F.col("ends_on") >= "2023-07-01")
    .withColumn("is_store_avg_2", F.avg("is_store").over(w))
    .withColumn("prev_is_store", F.lag("is_store", 2).over(w2))
    .withColumn("cumulative_is_store", F.sum("is_store").over(w3))
)

attributed = (
    metrics
    .alias("s")
    .join(df_promotions.alias("pr"), (F.col("s.channel").eqNullSafe(F.col("pr.channel"))) & (F.col("s.starts_on") >= F.col("pr.starts_on")) & (F.col("s.starts_on") < F.col("pr.ends_on") + F.expr("INTERVAL 7 DAYS")), "left")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

remaining_keys = (
    attributed
    .select("promo_code")
    .subtract(
        df_promotions
        .filter(F.col("discount_pct") >= 0.23)
        .select("promo_code")
    )
)

kept_rows = (
    attributed
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.promo_code") == F.col("k.promo_code")), "inner")
    .select("s.*")
)

scored = (
    kept_rows
    .select(
        "id",
        "channel",
        "ends_on",
        "is_store",
        "discount_pct",
        F.avg("cumulative_is_store").over(w4).alias("channel_avg_cumulative_is_store"),
        F.avg("is_store").over(w4).alias("channel_avg_is_store"),
        F.rank().over(w5).alias("promo_id_rank"),
    )
)

with_window = (
    scored
    .alias("a")
    .join(df_promotions.alias("pr"), (F.col("a.channel") == F.col("pr.channel")) & (F.col("a.ends_on") >= F.col("pr.starts_on")) & (F.col("a.ends_on") < F.col("pr.ends_on") + F.expr("INTERVAL 7 DAYS")), "left")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

matched = (
    with_window
    .alias("a")
    .join(df_promotions.alias("pr"), (F.col("a.channel") == F.col("pr.channel")) & (F.col("a.ends_on") >= F.col("pr.starts_on")) & (F.col("a.ends_on") < F.col("pr.ends_on") + F.expr("INTERVAL 14 DAYS")), "inner")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id_2"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct_2"),
    )
)

result = (
    matched
    .select(
        "channel",
        "ends_on",
        "is_store",
        "discount_pct",
        "channel_avg_cumulative_is_store",
        "channel_avg_is_store",
        "promo_id_rank",
        "promo_id",
        "promo_code",
    )
    .orderBy("ends_on", "promo_id")
)
