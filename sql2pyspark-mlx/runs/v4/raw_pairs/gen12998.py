from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("promo_code").orderBy(F.col("discount_pct").desc())
w2 = Window.partitionBy("promo_code").orderBy("discount_pct", "id")
w_recent_2 = Window.partitionBy("promo_code").orderBy("starts_on", "id").rowsBetween(-6, Window.currentRow)
w4 = Window.orderBy(F.col("discount_pct_percent_rank").desc(), "id")
w_hist = Window.orderBy("discount_pct_percent_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("running_discount_pct_percent_rank_rounded", "id")
w7 = Window.orderBy("least_discount_pct_avg_6_discount_pct_bucket", "id")
w8 = Window.orderBy("discount_pct_percent_rank_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.orderBy("discount_pct_percent_rank_bucket", "id")
w10 = Window.orderBy("starts_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_promotions
    .filter((F.col("starts_on") >= F.to_date(F.lit("2023-09-25"))) & (F.col("discount_pct") >= 0.19))
    .select("*", F.lower("channel").alias("channel_lower"))
)

scored = (
    base
    .withColumn("discount_pct_percent_rank", F.percent_rank().over(w_recent))
    .withColumn("discount_pct_bucket", F.ntile(5).over(w2))
    .withColumn("discount_pct_avg_6", F.avg("discount_pct").over(w_recent_2))
)

filtered = (
    scored
    .filter((F.col("starts_on") > F.lit("2024-02-24").cast("date")) & F.col("channel").isNotNull() & (F.col("channel") == "partner"))
    .select(
        "id",
        "starts_on",
        "discount_pct",
        "discount_pct_percent_rank",
        "discount_pct_bucket",
        "discount_pct_avg_6",
        F.col("discount_pct").cast("int").alias("discount_pct_int"),
        F.coalesce("channel", F.lit("OTHER")).alias("channel_clean"),
    )
)

filtered_promotions = (
    filtered
    .filter((F.col("starts_on") > F.to_date(F.lit("2023-09-03"))) & (F.col("discount_pct") <= 0.15))
    .select(
        "*",
        F.round("discount_pct_percent_rank", 2).alias("discount_pct_percent_rank_rounded"),
        F.least("discount_pct_avg_6", "discount_pct_bucket").alias("least_discount_pct_avg_6_discount_pct_bucket"),
    )
)

scored_promotions = (
    filtered_promotions
    .withColumn("discount_pct_percent_rank_bucket", F.ntile(10).over(w4))
    .withColumn("running_discount_pct_percent_rank_rounded", F.sum("discount_pct_percent_rank_rounded").over(w_hist))
)

enriched = (
    scored_promotions
    .select(
        "*",
        F.row_number().over(w6).alias("seq_no"),
        F.lag("discount_pct_int", 1).over(w7).alias("prev_discount_pct_int"),
        F.first("discount_pct_percent_rank_bucket", ignorenulls=True).over(w8).alias("first_known_discount_pct_percent_rank_bucket"),
        F.lead("running_discount_pct_percent_rank_rounded", 1).over(w9).alias("next_running_discount_pct_percent_rank_rounded"),
        F.first("running_discount_pct_percent_rank_rounded", ignorenulls=True).over(w10).alias("first_known_running_discount_pct_percent_rank_rounded"),
    )
)

result = (
    enriched
    .groupBy("seq_no")
    .agg(
        F.round(F.avg("discount_pct_int"), 2).alias("avg_discount_pct_int"),
        F.countDistinct("id").alias("distinct_ids"),
        F.min("discount_pct_percent_rank_bucket").alias("min_discount_pct_percent_rank_bucket"),
    )
)
