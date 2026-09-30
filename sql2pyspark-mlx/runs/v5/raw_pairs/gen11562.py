from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy(F.col("discount_pct").desc(), "id")
w_grp = Window.partitionBy("promo_code", "channel").orderBy("rank_in_group", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("ends_on", "id")
w4 = Window.orderBy("ends_on", "id").rowsBetween(-5, -1)
w5 = Window.orderBy("rank_in_group", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_promotions
    .filter(F.col("discount_pct").between(0.25, 0.28))
    .select("*", F.date_add("ends_on", 19).alias("ends_on_plus_19d"))
)

deduped = (
    filtered
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

enriched = (
    deduped
    .select(
        "id",
        "ends_on",
        "discount_pct",
        "ends_on_plus_19d",
        "rank_in_group",
        F.count("discount_pct").over(w_grp).alias("running_count_discount_pct"),
        F.min("discount_pct").over(w_grp).alias("running_min_discount_pct"),
        F.sum("discount_pct").over(w_grp).alias("running_sum_discount_pct"),
    )
)

with_windows = (
    enriched
    .select(
        "*",
        F.lead("running_sum_discount_pct").over(w3).alias("next_running_sum_discount_pct"),
        F.lag("running_sum_discount_pct").over(w3).alias("prev_running_sum_discount_pct"),
        F.lag("running_count_discount_pct").over(w3).alias("prev_running_count_discount_pct"),
        F.lead("running_count_discount_pct").over(w3).alias("next_running_count_discount_pct"),
        F.row_number().over(w3).alias("row_seq"),
        F.avg("discount_pct").over(w4).alias("discount_pct_avg_5_prior"),
    )
)

metrics = (
    with_windows
    .filter(F.col("next_running_sum_discount_pct") >= 1.23)
    .select(
        "*",
        F.avg("discount_pct_avg_5_prior").over(w5).alias("running_avg_discount_pct_avg_5_prior"),
        F.min("discount_pct_avg_5_prior").over(w5).alias("running_min_discount_pct_avg_5_prior"),
        F.max("discount_pct_avg_5_prior").over(w5).alias("running_max_discount_pct_avg_5_prior"),
    )
)

result = (
    metrics
    .groupBy(
        F.date_trunc("MONTH", "ends_on_plus_19d").alias("activity_month"),
        F.when(F.col("next_running_count_discount_pct") >= 18, "PREMIUM").otherwise("STANDARD").alias("next_running_count_discount_pct_tier"),
        F.date_trunc("WEEK", "ends_on_plus_19d").alias("week_start"),
    )
    .agg(
        F.min("running_min_discount_pct").alias("min_running_min_discount_pct"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("prev_running_sum_discount_pct", [0.25, 0.5, 0.75]).alias("prev_running_sum_discount_pct_quartiles"),
    )
    .orderBy(F.col("next_running_count_discount_pct_tier").desc())
)
