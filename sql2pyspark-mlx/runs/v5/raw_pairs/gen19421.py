from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("channel").orderBy("starts_on", "id")
w2 = Window.partitionBy("channel").orderBy("ends_on", "id").rowsBetween(-6, Window.currentRow)

with_windows = (
    df_promotions
    .select(
        "id",
        "starts_on",
        "discount_pct",
        F.lag("discount_pct").over(w_recent).alias("prev_discount_pct"),
        F.lead("discount_pct").over(w_recent).alias("next_discount_pct"),
        F.row_number().over(w_recent).alias("seq_no"),
        F.avg("discount_pct").over(w2).alias("discount_pct_avg_6"),
    )
)

result = (
    with_windows
    .withColumn("discount_pct_tier", F.when(F.col("discount_pct") >= 0.29, "PREMIUM").otherwise("STANDARD"))
    .rollup("discount_pct_tier")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("event_count"),
        F.count("*").alias("event_count_2"),
        F.count("*").alias("num_rows"),
        F.grouping_id().alias("grouping_id"),
    )
    .orderBy(F.col("event_count").desc())
)
