from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("plan")
w_ordered = Window.partitionBy("currency")
w_recent = Window.orderBy(F.col("currency_sum_val").desc_nulls_last())
w4 = Window.partitionBy("plan", "currency")

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode_outer("discounts").alias("val"),
    )
)

scored = (
    lines
    .filter(F.col("val") <= 33)
    .select(
        "*",
        F.sum("val").over(w_hist).alias("plan_sum_val"),
        F.sum("val").over(w_ordered).alias("currency_sum_val"),
        F.sum("val").over(w_ordered).alias("currency_sum_val_2"),
    )
)

enriched = (
    scored
    .withColumn("currency_sum_val_rank", F.rank().over(w_recent))
    .withColumn("plan_sum_currency_sum_val", F.sum("currency_sum_val").over(w4))
    .withColumn("plan_max_plan_sum_val", F.max("plan_sum_val").over(w_hist))
    .withColumn("plan_stddev_samp_val", F.stddev_samp("val").over(w_hist))
)

valid = (
    enriched
    .filter(F.col("currency_sum_val_rank") < 7)
    .withColumn("plan_stddev_samp_val_times_currency_sum_val_rank", (F.col("plan_stddev_samp_val") * F.col("currency_sum_val_rank")))
)

enriched_price_tiers = (
    valid
    .select(
        "id",
        "plan",
        "currency",
        "val",
        "plan_sum_val",
        "plan_max_plan_sum_val",
        "plan_stddev_samp_val",
        "plan_stddev_samp_val_times_currency_sum_val_rank",
        F.count("val").over(w4).alias("plan_count_val"),
    )
)

stats = (
    enriched_price_tiers
    .groupBy(
        F.when(F.col("plan_sum_val") >= 338, "LARGE").otherwise("SMALL").alias("plan_sum_val_tier"),
    )
    .agg(
        F.count(F.when(F.col("currency") == "USD", 1)).alias("usd_count"),
        F.round(F.sum("val") / F.nullif(F.count(F.when(F.col("currency") == "USD", 1)), F.lit(0)), 2).alias("val_per_usd"),
        F.percentile_approx("plan_sum_val", 0.5).alias("plan_sum_val_p50"),
        F.avg(F.when(F.col("plan") == "pro", F.col("plan_sum_val"))).alias("pro_avg_plan_sum_val"),
    )
)

result = (
    stats
    .select(
        "plan_sum_val_tier",
        "usd_count",
        "val_per_usd",
        "plan_sum_val_p50",
        "pro_avg_plan_sum_val",
    )
)
