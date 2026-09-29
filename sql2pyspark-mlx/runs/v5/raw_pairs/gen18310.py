from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("promo_code").orderBy("starts_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp_2 = Window.partitionBy("channel", "promo_code").orderBy(F.col("discount_pct").desc(), "id")
w3 = Window.orderBy("running_avg_discount_pct", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("channel").orderBy(F.col("discount_pct_times_promotion_discount_pct").desc())
w5 = Window.partitionBy("promotion_promo_code").orderBy(F.col("running_max_discount_pct").desc(), "id", "promo_id")

enriched = (
    df_promotions
    .select(
        "*",
        F.max("discount_pct").over(w_grp).alias("running_max_discount_pct"),
        F.count("discount_pct").over(w_grp).alias("running_count_discount_pct"),
        F.sum("discount_pct").over(w_grp).alias("running_sum_discount_pct"),
        F.avg("discount_pct").over(w_grp).alias("running_avg_discount_pct"),
        F.row_number().over(w_grp_2).alias("seq_no"),
    )
)

matched = (
    enriched
    .alias("src")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("src.channel"), F.lit("web")) == F.col("pr.channel")) & (F.col("src.ends_on") >= F.col("pr.starts_on")) & (F.col("src.ends_on") <= F.date_add(F.col("pr.ends_on"), 10)), "left")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

cleaned = (
    matched
    .filter(F.col("ends_on") >= F.to_date(F.lit("2023-12-04")))
    .withColumn("discount_pct_times_promotion_discount_pct", (F.col("discount_pct") * F.col("promotion_discount_pct")))
    .withColumn("discount_pct_plus_promo_id", (F.col("discount_pct") + F.col("promo_id")))
)

scored = (
    cleaned
    .select(
        "id",
        "channel",
        "promo_id",
        "promotion_promo_code",
        "discount_pct_times_promotion_discount_pct",
        F.first("running_sum_discount_pct", ignorenulls=True).over(w3).alias("first_known_running_sum_discount_pct"),
        F.cume_dist().over(w4).alias("discount_pct_times_promotion_discount_pct_cume_dist"),
        F.row_number().over(w5).alias("row_seq"),
    )
)

result = (
    scored
    .groupBy("promo_id")
    .agg(
        F.round(F.sum("row_seq") / F.nullif(F.count(F.when(F.col("channel") == "mobile", 1)), F.lit(0)), 2).alias("row_seq_per_mobile"),
        F.count(F.when(F.col("promotion_promo_code") == "VIP", 1)).alias("vip_count"),
        F.avg(F.when(F.col("channel") == "web", F.col("promo_id"))).alias("web_avg_promo_id"),
    )
    .orderBy(F.col("vip_count").asc_nulls_last())
)
