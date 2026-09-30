from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy(F.col("ends_on").desc(), "id")
w_recent = Window.partitionBy("promo_code").orderBy(F.col("starts_on_year").desc())
w3 = Window.partitionBy("promo_code").orderBy("starts_on_year", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("promo_code")
w_ordered = Window.orderBy(F.col("starts_on_year").desc_nulls_last())
w6 = Window.partitionBy("promo_code").orderBy("starts_on_year_dense_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("promo_code").orderBy("starts_on_year_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("promo_code").orderBy(F.col("cumulative_starts_on_year").desc_nulls_last())
w9 = Window.partitionBy("promo_code").orderBy(F.col("starts_on_year_rank").desc(), "id")
w_grp = Window.partitionBy("promo_code").orderBy("row_seq", "id").rowsBetween(-5, Window.currentRow)
w_hist = Window.partitionBy("promo_code").orderBy("starts_on", "id").rowsBetween(-2, Window.currentRow)
w12 = Window.partitionBy("promo_code").orderBy("ends_on", "id")

base = (
    df_promotions
    .filter((F.col("ends_on") > F.to_date(F.lit("2024-06-09"))) & F.col("channel").isNotNull())
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        F.year("starts_on").alias("starts_on_year"),
    )
)

valid = (
    base
    .filter((F.col("ends_on") > F.lit("2023-10-21").cast("date")) & F.col("starts_on").between("2024-02-01", "2024-07-01"))
    .select(
        "id",
        "promo_code",
        "starts_on",
        "ends_on",
        "starts_on_year",
        (F.col("discount_pct") + F.col("starts_on_year")).alias("discount_pct_plus_starts_on_year"),
    )
)

scored = (
    valid
    .withColumn("row_seq", F.row_number().over(w))
    .withColumn("starts_on_year_rank", F.rank().over(w_recent))
    .withColumn("cumulative_starts_on_year", F.sum("starts_on_year").over(w3))
    .withColumn("promo_code_max_starts_on_year", F.max("starts_on_year").over(w4))
    .withColumn("starts_on_year_dense_rank", F.dense_rank().over(w_ordered))
)

scored_promotions = (
    scored
    .select(
        "*",
        F.sum("starts_on_year_rank").over(w6).alias("cumulative_starts_on_year_rank"),
        F.sum("cumulative_starts_on_year").over(w4).alias("promo_code_sum_cumulative_starts_on_year"),
        F.first("starts_on_year", ignorenulls=True).over(w7).alias("first_known_starts_on_year"),
        F.cume_dist().over(w8).alias("cumulative_starts_on_year_cume_dist"),
        F.sum("starts_on_year_rank").over(w6).alias("running_starts_on_year_rank"),
    )
)

latest = (
    scored_promotions
    .withColumn("latest_rank", F.row_number().over(w9))
    .filter(F.col("latest_rank") == 1)
)

enriched = (
    latest
    .withColumn("promo_code_max_starts_on_year_avg_5", F.avg("promo_code_max_starts_on_year").over(w_grp))
    .withColumn("cumulative_starts_on_year_cume_dist_avg_2", F.avg("cumulative_starts_on_year_cume_dist").over(w_hist))
    .withColumn("promo_code_sum_starts_on_year_rank", F.sum("starts_on_year_rank").over(w4))
    .withColumn("row_seq_share", F.round((F.col("row_seq") / F.sum("row_seq").over(w4)), 4))
)

latest_promotions = (
    enriched
    .withColumn("latest_rank_2", F.row_number().over(w12))
    .filter(F.col("latest_rank_2") == 1)
)

stats = (
    latest_promotions
    .groupBy("promo_code")
    .agg(
        F.countDistinct("promo_code").alias("distinct_promo_codes"),
        F.countDistinct("promo_code").alias("distinct_promo_codes_2"),
        F.sort_array(F.collect_set("promo_code")).alias("promo_code_list"),
    )
)

result = (
    stats
    .select("promo_code", "distinct_promo_codes")
    .orderBy(F.col("distinct_promo_codes").asc_nulls_last(), F.col("promo_code").desc())
)
