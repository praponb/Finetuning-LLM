from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy("ends_on", "id")
w_grp = Window.orderBy(F.col("ends_on").desc(), "id")
w_ordered = Window.partitionBy("promo_code").orderBy(F.col("ends_on").desc(), "id")
w4 = Window.orderBy("seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy(F.col("seq_no_2").desc(), "id")
w6 = Window.orderBy("seq_no", "id")
w7 = Window.orderBy("seq_no_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.orderBy("running_event_seq", "id").rowsBetween(-5, Window.currentRow)
w9 = Window.orderBy("seq_no_2", "id").rowsBetween(-5, Window.currentRow)
w_part = Window.orderBy("seq_no_2_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_promotions
    .filter(F.col("ends_on") <= "2025-04-19")
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        F.concat_ws("-", "channel", "promo_code").alias("channel_promo_code_key"),
    )
)

deduped = (
    filtered
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "promo_code", "starts_on", "ends_on", "channel_promo_code_key")
)

enriched = (
    deduped
    .select(
        "id",
        "channel_promo_code_key",
        F.row_number().over(w_grp).alias("seq_no"),
        F.row_number().over(w_ordered).alias("event_seq"),
        F.row_number().over(w_ordered).alias("seq_no_2"),
    )
)

metrics = (
    enriched
    .filter(F.col("event_seq").between(3, 5))
    .select(
        "*",
        F.last("seq_no_2", ignorenulls=True).over(w4).alias("last_known_seq_no_2"),
        F.ntile(10).over(w5).alias("seq_no_2_bucket"),
        F.ntile(10).over(w6).alias("seq_no_bucket"),
        F.sum("event_seq").over(w7).alias("running_event_seq"),
    )
)

enriched_promotions = (
    metrics
    .select(
        "*",
        F.avg("seq_no").over(w8).alias("seq_no_avg_5"),
        F.avg("seq_no_2").over(w9).alias("seq_no_2_avg_5"),
        F.sum("seq_no_bucket").over(w_part).alias("cumulative_seq_no_bucket"),
    )
)

result = (
    enriched_promotions
    .groupBy("seq_no")
    .agg(
        F.percentile_approx("seq_no_bucket", 0.95).alias("seq_no_bucket_p95"),
        F.percentile_approx("seq_no_avg_5", [0.25, 0.5, 0.75]).alias("seq_no_avg_5_quartiles"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy(F.col("seq_no_bucket_p95").desc_nulls_last())
)
