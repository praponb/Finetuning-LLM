from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("starts_on", "id")
w2 = Window.partitionBy("channel").orderBy(F.col("discount_pct").desc())
w3 = Window.orderBy(F.col("discount_pct").desc())
w4 = Window.partitionBy("channel").orderBy("ends_on", "id").rowsBetween(-5, Window.currentRow)
w5 = Window.partitionBy("channel").orderBy("starts_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("promo_code", "channel")
w7 = Window.partitionBy("promo_code").orderBy("ends_on", "id").rowsBetween(-3, -1)
w8 = Window.partitionBy("promo_code", "channel").orderBy("discount_pct_dense_rank", "id")
w9 = Window.partitionBy("promo_code", "channel").orderBy("discount_pct_dense_rank", "id").rowsBetween(-3, -1)
w10 = Window.orderBy("starts_on", "id").rowsBetween(-2, Window.currentRow)
w11 = Window.partitionBy("channel").orderBy(F.col("promo_code_count_discount_pct_dense_rank").desc(), "id")
w12 = Window.partitionBy("channel").orderBy("next_discount_pct_avg_5", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w13 = Window.partitionBy("promo_code").orderBy("next_discount_pct", "id").rowsBetween(-3, Window.currentRow)
w14 = Window.partitionBy("promo_code").orderBy(F.col("next_discount_pct_avg_5").asc_nulls_first(), "id")

scored = (
    df_promotions
    .withColumn("next_discount_pct", F.lead("discount_pct", 2).over(w))
    .withColumn("discount_pct_percent_rank", F.percent_rank().over(w2))
    .withColumn("discount_pct_dense_rank", F.dense_rank().over(w3))
)

metrics = (
    scored
    .select(
        "*",
        F.avg("next_discount_pct").over(w4).alias("next_discount_pct_avg_5"),
        F.first("channel", ignorenulls=True).over(w5).alias("first_known_channel"),
        F.count("discount_pct_dense_rank").over(w6).alias("promo_code_count_discount_pct_dense_rank"),
        F.avg("discount_pct_dense_rank").over(w7).alias("discount_pct_dense_rank_avg_3_prior"),
        F.ntile(10).over(w8).alias("discount_pct_dense_rank_bucket"),
    )
)

scored_promotions = (
    metrics
    .select(
        "*",
        F.avg("next_discount_pct").over(w9).alias("next_discount_pct_avg_3_prior"),
        F.avg("discount_pct_dense_rank").over(w10).alias("discount_pct_dense_rank_avg_2"),
        F.ntile(4).over(w11).alias("promo_code_count_discount_pct_dense_rank_bucket"),
        F.last("next_discount_pct", ignorenulls=True).over(w12).alias("last_known_next_discount_pct"),
        F.avg("promo_code_count_discount_pct_dense_rank").over(w13).alias("promo_code_count_discount_pct_dense_rank_avg_3"),
    )
)

deduped = (
    scored_promotions
    .withColumn("row_num", F.row_number().over(w14))
    .filter(F.col("row_num") == 1)
)

result = (
    deduped
    .select(
        "id",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        "next_discount_pct",
        "discount_pct_percent_rank",
        "discount_pct_dense_rank",
        "first_known_channel",
    )
)
