from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("promo_code")
w_hist = Window.partitionBy("promo_code").orderBy("channel_length", "id").rowsBetween(-2, Window.currentRow)
w4 = Window.orderBy("discount_pct", "id")
w5 = Window.partitionBy("promo_code").orderBy(F.col("ends_on").desc(), "id")
w6 = Window.orderBy(F.col("starts_on").desc(), "id")
w7 = Window.partitionBy("promotion_promo_code").orderBy(F.col("promo_code_sum_discount_pct").desc())
w8 = Window.partitionBy("channel").orderBy(F.col("seq_no").asc_nulls_last(), "id", "promo_id")
w9 = Window.partitionBy("promo_code").orderBy("starts_on", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("promo_code", "channel").orderBy(F.col("starts_on").desc(), "id", "promo_id")

base = (
    df_promotions
    .filter(F.year("ends_on") >= 2024)
    .withColumn("channel_prefix", F.substring("channel", 1, 4))
    .withColumn("channel_length", F.length("channel"))
    .withColumn("resolved_channel", F.coalesce("channel", F.lit("OTHER")))
)

with_windows = (
    base
    .withColumn("running_channel_length", F.sum("channel_length").over(w))
    .withColumn("promo_code_sum_discount_pct", F.sum("discount_pct").over(w2))
    .withColumn("channel_length_avg_2", F.avg("channel_length").over(w_hist))
)

enriched = (
    with_windows
    .select(
        "*",
        F.row_number().over(w4).alias("row_seq"),
        F.row_number().over(w5).alias("row_seq_2"),
        F.row_number().over(w6).alias("seq_no"),
    )
)

attributed = (
    enriched
    .alias("s")
    .join(df_promotions.alias("pr"), (F.col("s.channel").eqNullSafe(F.col("pr.channel"))) & (F.col("s.ends_on") >= F.col("pr.starts_on")) & (F.col("s.ends_on") <= F.date_add(F.col("pr.ends_on"), 10)), "left")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

ranked = (
    attributed
    .withColumn("row_num", F.rank().over(w7))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

ranked_promotions = (
    ranked
    .withColumn("rank_in_group", F.row_number().over(w8))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

result = (
    ranked_promotions
    .filter(F.col("promo_id") != 9)
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "running_channel_length",
        "promo_code_sum_discount_pct",
        "row_seq_2",
        "seq_no",
        "promo_id",
        "promotion_promo_code",
        "promotion_discount_pct",
        F.sum("running_channel_length").over(w9).alias("running_sum_running_channel_length"),
        F.max("running_channel_length").over(w9).alias("running_max_running_channel_length"),
        F.min("running_channel_length").over(w9).alias("running_min_running_channel_length"),
        F.row_number().over(w10).alias("seq_no_2"),
    )
)
