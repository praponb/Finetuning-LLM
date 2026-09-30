from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy(F.col("ends_on").desc(), "id")
w2 = Window.partitionBy("promo_code").orderBy("starts_on_period", "id")
w3 = Window.partitionBy("promo_code").orderBy("starts_on_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("promo_code").orderBy("session_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_promotions
    .filter(F.col("discount_pct").between(0.15, 0.26))
    .select(
        "id",
        "promo_code",
        "ends_on",
        "discount_pct",
        F.date_trunc("year", "starts_on").alias("starts_on_period"),
        F.upper("channel").alias("channel_upper"),
        F.coalesce("channel", F.lit("N/A")).alias("resolved_channel"),
    )
)

ranked = (
    cleaned
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

gaps = (
    ranked
    .withColumn("session_start_flag", F.when(F.col("starts_on_period") - F.lag("starts_on_period").over(w2) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

sessionized = gaps.withColumn("session_no", F.sum("session_start_flag").over(w3))

enriched = (
    sessionized
    .select(
        "id",
        "promo_code",
        "ends_on",
        "discount_pct",
        "channel_upper",
        "resolved_channel",
        "session_no",
        F.avg("session_start_flag").over(w_running).alias("running_avg_session_start_flag"),
        F.max("session_start_flag").over(w_running).alias("running_max_session_start_flag"),
        F.sum("session_start_flag").over(w_running).alias("running_sum_session_start_flag"),
    )
)

result = (
    enriched
    .groupBy(F.date_trunc("week", "ends_on").alias("week_start"))
    .agg(
        F.round(F.avg("discount_pct"), 2).alias("avg_discount_pct"),
        F.percentile_approx("running_avg_session_start_flag", [0.25, 0.5, 0.75]).alias("running_avg_session_start_flag_quartiles"),
        F.countDistinct("promo_code").alias("distinct_promo_codes"),
    )
)
