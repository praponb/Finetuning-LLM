from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("ends_on")
w2 = Window.partitionBy("channel", "promo_code").orderBy("rank_in_group", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("promo_code").orderBy("starts_on", "id")
w4 = Window.partitionBy("promo_code").orderBy("running_sum_discount_pct", "id")
w5 = Window.partitionBy("channel").orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_promotions
    .filter((F.col("starts_on") > F.lit("2023-10-06").cast("date")) & F.col("channel").isin(["mobile", "web"]))
    .select(
        "*",
        F.dayofmonth("ends_on").alias("ends_on_day"),
        F.coalesce("channel", F.lit("UNKNOWN")).alias("resolved_channel"),
    )
)

latest = (
    valid
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
)

metrics = (
    latest
    .filter(F.month("ends_on").isin([1, 5, 12]))
    .select(
        "*",
        F.sum("discount_pct").over(w2).alias("running_sum_discount_pct"),
        F.avg("discount_pct").over(w2).alias("running_avg_discount_pct"),
        F.max("discount_pct").over(w2).alias("running_max_discount_pct"),
    )
)

with_windows = (
    metrics
    .select(
        "id",
        "promo_code",
        "resolved_channel",
        "running_avg_discount_pct",
        F.lead("running_max_discount_pct", 1).over(w3).alias("next_running_max_discount_pct"),
        F.lead("running_avg_discount_pct", 1).over(w4).alias("next_running_avg_discount_pct"),
        F.sum("ends_on_day").over(w5).alias("cumulative_ends_on_day"),
    )
)

result = (
    with_windows
    .groupBy("promo_code")
    .agg(
        F.sort_array(F.collect_set("promo_code")).alias("promo_code_list"),
        F.size(F.collect_set("promo_code")).alias("num_promo_codes"),
        F.count(F.when(F.col("promo_code") == "WELCOME5", 1)).alias("welcome5_count"),
        F.round(F.sum("next_running_max_discount_pct") / F.nullif(F.count(F.when(F.col("promo_code") == "SPRING10", 1)), F.lit(0)), 2).alias("next_running_max_discount_pct_per_spring10"),
    )
    .filter(F.col("next_running_max_discount_pct_per_spring10") < 1000000)
    .orderBy("promo_code")
)
