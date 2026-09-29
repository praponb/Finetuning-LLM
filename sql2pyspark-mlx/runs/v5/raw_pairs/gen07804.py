from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel", "promo_code").orderBy("starts_on", "id")
w2 = Window.partitionBy("channel").orderBy("discount_pct", "id").rowsBetween(-3, -1)
w3 = Window.orderBy("starts_on", "id").rowsBetween(-2, -1)
w4 = Window.partitionBy("channel").orderBy("discount_pct", "id")
w5 = Window.partitionBy("promo_code").orderBy(F.col("discount_pct_avg_2_prior").asc_nulls_last(), "id")
w6 = Window.partitionBy("promo_code").orderBy("dedup_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("promo_code").orderBy(F.col("discount_pct").desc(), "id")
w8 = Window.partitionBy("promo_code").orderBy(F.col("starts_on").desc())
w_part = Window.orderBy("starts_on", "id").rowsBetween(-6, -1)
w10 = Window.orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w11 = Window.partitionBy("week_start").orderBy(F.col("avg_running_sum_discount_pct").desc())

enriched = (
    df_promotions
    .select(
        "*",
        F.lag("discount_pct", 2).over(w).alias("prev_discount_pct"),
        F.avg("discount_pct").over(w2).alias("discount_pct_avg_3_prior"),
        F.avg("discount_pct").over(w3).alias("discount_pct_avg_2_prior"),
    )
)

ranked = (
    enriched
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select("id", "promo_code", "starts_on", "discount_pct", "discount_pct_avg_2_prior")
)

ranked_promotions = (
    ranked
    .withColumn("dedup_rank", F.row_number().over(w5))
    .filter(F.col("dedup_rank") == 1)
)

enriched_promotions = (
    ranked_promotions
    .select(
        "id",
        "promo_code",
        "starts_on",
        "discount_pct",
        F.min("discount_pct").over(w6).alias("running_min_discount_pct"),
        F.sum("discount_pct").over(w6).alias("running_sum_discount_pct"),
        F.avg("discount_pct").over(w6).alias("running_avg_discount_pct"),
    )
)

ranked_promotions_2 = (
    enriched_promotions
    .withColumn("latest_rank", F.row_number().over(w7))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

ranked_promotions_3 = (
    ranked_promotions_2
    .withColumn("row_num", F.rank().over(w8))
    .filter(F.col("row_num") == 1)
    .select(
        "id",
        "starts_on",
        "discount_pct",
        "running_min_discount_pct",
        "running_sum_discount_pct",
    )
)

scored = (
    ranked_promotions_3
    .filter(F.year("starts_on") >= 2023)
    .select(
        "*",
        F.avg("running_sum_discount_pct").over(w_part).alias("running_sum_discount_pct_avg_6_prior"),
        F.first("running_sum_discount_pct", ignorenulls=True).over(w10).alias("first_known_running_sum_discount_pct"),
    )
)

result = (
    scored
    .groupBy(
        F.date_trunc("WEEK", "starts_on").alias("week_start"),
        F.when(F.col("running_min_discount_pct") >= 0.28, "LARGE").otherwise("SMALL").alias("running_min_discount_pct_tier"),
    )
    .agg(
        F.percentile_approx("running_sum_discount_pct_avg_6_prior", [0.25, 0.5, 0.75]).alias("running_sum_discount_pct_avg_6_prior_quartiles"),
        F.max("running_min_discount_pct").alias("max_running_min_discount_pct"),
        F.round(F.avg("running_sum_discount_pct"), 2).alias("avg_running_sum_discount_pct"),
    )
    .filter(F.col("avg_running_sum_discount_pct") >= 0)
    .withColumn("avg_running_sum_discount_pct_rank", F.rank().over(w11))
    .orderBy(F.col("max_running_min_discount_pct").desc_nulls_first())
)
