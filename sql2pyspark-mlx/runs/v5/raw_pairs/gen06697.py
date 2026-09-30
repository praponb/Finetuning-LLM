from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy("ends_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("starts_on", "id")
w3 = Window.orderBy("running_max_discount_pct", "id").rowsBetween(-2, Window.currentRow)
w_ordered = Window.partitionBy("promo_code").orderBy("running_avg_discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("seq_no_2_tier").orderBy(F.col("distinct_ids_2").desc())

enriched = (
    df_promotions
    .filter(F.col("promo_code") == "VIP")
    .select(
        "id",
        "promo_code",
        "starts_on",
        "ends_on",
        F.max("discount_pct").over(w).alias("running_max_discount_pct"),
        F.count("discount_pct").over(w).alias("running_count_discount_pct"),
        F.sum("discount_pct").over(w).alias("running_sum_discount_pct"),
        F.avg("discount_pct").over(w).alias("running_avg_discount_pct"),
    )
)

with_windows = (
    enriched
    .select(
        "id",
        "starts_on",
        "running_max_discount_pct",
        "running_sum_discount_pct",
        F.row_number().over(w2).alias("seq_no"),
        F.avg("running_sum_discount_pct").over(w3).alias("running_sum_discount_pct_avg_2"),
        F.sum("running_avg_discount_pct").over(w_ordered).alias("running_running_avg_discount_pct"),
    )
)

scored = (
    with_windows
    .select(
        "*",
        F.lag("running_max_discount_pct").over(w2).alias("prev_running_max_discount_pct"),
        F.lead("running_sum_discount_pct").over(w2).alias("next_running_sum_discount_pct"),
        F.row_number().over(w2).alias("seq_no_2"),
    )
)

cleaned = (
    scored
    .filter(F.col("starts_on") >= F.lit("2023-05-07").cast("date"))
    .select(
        "id",
        "starts_on",
        "running_sum_discount_pct",
        "seq_no",
        "running_sum_discount_pct_avg_2",
        "running_running_avg_discount_pct",
        "prev_running_max_discount_pct",
        "seq_no_2",
        F.greatest("seq_no_2", "running_max_discount_pct").alias("greatest_seq_no_2_running_max_discount_pct"),
    )
)

result = (
    cleaned
    .groupBy(
        F.when(F.col("seq_no_2") >= 7, "HIGH").otherwise("LOW").alias("seq_no_2_tier"),
        F.date_trunc("week", "starts_on").alias("week_start"),
    )
    .agg(
        F.avg("prev_running_max_discount_pct").alias("avg_prev_running_max_discount_pct"),
        F.avg("prev_running_max_discount_pct").alias("avg_prev_running_max_discount_pct_2"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.avg("seq_no"), 2).alias("avg_seq_no"),
        F.countDistinct("id").alias("distinct_ids_2"),
        F.countDistinct("id").alias("distinct_ids_3"),
    )
    .withColumn("distinct_ids_2_rank", F.rank().over(w5))
)
