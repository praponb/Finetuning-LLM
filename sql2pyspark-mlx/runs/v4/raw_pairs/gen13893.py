from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("starts_on", "id")
w_recent = Window.partitionBy("promo_code").orderBy(F.col("discount_pct").desc())
w4 = Window.partitionBy("promo_code").orderBy("starts_on", "id")
w5 = Window.partitionBy("discount_pct_rank_tier").orderBy(F.col("num_channels").desc())

valid = (
    df_promotions
    .filter((F.col("starts_on") > F.to_date(F.lit("2024-01-26"))) & F.col("channel").isNotNull())
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "discount_pct",
        F.month("starts_on").alias("starts_on_month"),
    )
)

enriched = (
    valid
    .withColumn("cumulative_discount_pct", F.sum("discount_pct").over(w))
    .withColumn("prev_starts_on_month", F.lag("starts_on_month", 1).over(w2))
    .withColumn("discount_pct_rank", F.rank().over(w_recent))
)

deduped = (
    enriched
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
)

summary = (
    deduped
    .groupBy(
        F.when(F.col("discount_pct_rank") >= 8, "LARGE").otherwise("SMALL").alias("discount_pct_rank_tier"),
        F.when(F.col("discount_pct") >= 0.34, "PREMIUM").otherwise("STANDARD").alias("discount_pct_tier"),
    )
    .agg(
        F.size(F.collect_set("channel")).alias("num_channels"),
        F.avg(F.when(F.col("channel") == "web", F.col("prev_starts_on_month"))).alias("web_avg_prev_starts_on_month"),
        F.count(F.when(F.col("channel") == "store", 1)).alias("store_count"),
        F.count("*").alias("cnt"),
        F.sort_array(F.collect_set("promo_code")).alias("promo_code_list"),
        F.size(F.collect_set("channel")).alias("num_channels_2"),
    )
    .withColumn("num_channels_rank", F.rank().over(w5))
)

result = (
    summary
    .select(
        "discount_pct_rank_tier",
        "discount_pct_tier",
        "num_channels",
        "store_count",
        "cnt",
        "promo_code_list",
        "num_channels_2",
    )
)
