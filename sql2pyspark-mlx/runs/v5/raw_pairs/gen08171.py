from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy("discount_pct", "id")
w2 = Window.partitionBy("channel", "promo_code").orderBy("starts_on", "id").rowsBetween(-2, Window.currentRow)
w3 = Window.orderBy("ends_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("promo_code", "channel").orderBy("starts_on", "id")
w_running = Window.partitionBy("promo_code").orderBy("days_since_ends_on_avg_2", "id").rowsBetween(-2, Window.currentRow)
w6 = Window.partitionBy("promo_code").orderBy("days_since_ends_on_avg_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("promo_code").orderBy("last3_max_days_since_ends_on_avg_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("promo_code").orderBy(F.col("ends_on").desc(), "id")

filtered = (
    df_promotions
    .filter(F.col("channel").isNotNull() & (F.col("ends_on") > F.to_date(F.lit("2023-08-02"))))
    .select("*", F.datediff(F.current_date(), "ends_on").alias("days_since_ends_on"))
)

ranked = (
    filtered
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

enriched = (
    ranked
    .filter(F.col("channel").isin(["mobile", "web", "partner"]))
    .select(
        "id",
        "promo_code",
        "ends_on",
        F.avg("days_since_ends_on").over(w2).alias("days_since_ends_on_avg_2"),
        F.sum("days_since_ends_on").over(w3).alias("running_days_since_ends_on"),
        F.lead("days_since_ends_on", 1).over(w4).alias("next_days_since_ends_on"),
    )
)

scored = (
    enriched
    .select(
        "id",
        "promo_code",
        "ends_on",
        "days_since_ends_on_avg_2",
        "running_days_since_ends_on",
        F.count("days_since_ends_on_avg_2").over(w_running).alias("last3_count_days_since_ends_on_avg_2"),
        F.max("days_since_ends_on_avg_2").over(w_running).alias("last3_max_days_since_ends_on_avg_2"),
        F.min("days_since_ends_on_avg_2").over(w_running).alias("last3_min_days_since_ends_on_avg_2"),
        F.sum("days_since_ends_on_avg_2").over(w_running).alias("last3_sum_days_since_ends_on_avg_2"),
    )
)

enriched_promotions = (
    scored
    .withColumn("cumulative_last3_max_days_since_ends_on_avg_2", F.sum("last3_max_days_since_ends_on_avg_2").over(w6))
    .withColumn("running_last3_sum_days_since_ends_on_avg_2", F.sum("last3_sum_days_since_ends_on_avg_2").over(w7))
    .withColumn("row_seq", F.row_number().over(w8))
)

result = (
    enriched_promotions
    .groupBy("promo_code")
    .agg(
        F.percentile_approx("last3_min_days_since_ends_on_avg_2", [0.25, 0.5, 0.75]).alias("last3_min_days_since_ends_on_avg_2_quartiles"),
        F.count(F.when(F.col("promo_code") == "SPRING10", 1)).alias("spring10_rows"),
        F.countDistinct("promo_code").alias("distinct_promo_codes"),
    )
    .orderBy("distinct_promo_codes")
)
