from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promotion_promo_code", "channel").orderBy("starts_on", "id", "promo_id")
w2 = Window.partitionBy("promo_code").orderBy("promotion_discount_pct", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("promo_code").orderBy("promo_id", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("promotion_promo_code").orderBy(F.col("discount_pct_int_per_discount_pct").desc(), "id", "promo_id")

matched = (
    df_promotions
    .alias("s")
    .join(df_promotions.alias("pr"), (F.col("s.channel") == F.col("pr.channel")) & (F.col("s.starts_on") >= F.col("pr.starts_on")) & (F.col("s.starts_on") < F.col("pr.ends_on") + F.expr("INTERVAL 7 DAYS")), "inner")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

filtered = (
    matched
    .filter(~F.col("promo_code").isin(["BUNDLE", "WELCOME5", "VIP"]))
    .withColumn("discount_pct_int", F.col("discount_pct").cast("int"))
    .withColumn("resolved_channel", F.coalesce("channel", F.lit("OTHER")))
)

filtered_promotions = (
    filtered
    .filter(F.col("channel").isNotNull())
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "promo_id",
        "promotion_promo_code",
        "promotion_discount_pct",
        "discount_pct_int",
        F.round((F.col("discount_pct_int") / F.col("discount_pct")), 2).alias("discount_pct_int_per_discount_pct"),
    )
)

with_windows = (
    filtered_promotions
    .withColumn("seq_no", F.row_number().over(w))
    .withColumn("running_discount_pct_int_per_discount_pct", F.sum("discount_pct_int_per_discount_pct").over(w2))
    .withColumn("first_known_promotion_discount_pct", F.first("promotion_discount_pct", ignorenulls=True).over(w_hist))
    .withColumn("event_seq", F.row_number().over(w4))
)

result = (
    with_windows
    .groupBy("promo_id", "promotion_promo_code")
    .agg(
        F.percentile_approx("discount_pct_int", [0.25, 0.5, 0.75]).alias("discount_pct_int_quartiles"),
        F.avg("promo_id").alias("avg_promo_id"),
        F.count("*").alias("row_count"),
    )
)
