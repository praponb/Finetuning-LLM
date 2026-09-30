from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("discount_pct", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("promo_code", "channel").orderBy("starts_on", "id")

with_windows = (
    df_promotions
    .select(
        "*",
        F.avg("discount_pct").over(w).alias("discount_pct_avg_2"),
        F.row_number().over(w2).alias("event_seq"),
    )
)

result = (
    with_windows
    .groupBy(F.date_trunc("month", "ends_on").alias("activity_month"))
    .agg(
        F.max("event_seq").alias("max_event_seq"),
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
        F.count_if(F.col("channel") == "store").alias("store_events"),
        F.percentile_approx("event_seq", [0.25, 0.5, 0.75]).alias("event_seq_quartiles"),
        F.avg(F.when(F.col("channel") == "partner", F.col("event_seq"))).alias("partner_avg_event_seq"),
        F.count(F.when(F.col("promo_code") == "VIP", 1)).alias("vip_count"),
    )
)
