from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("discount_pct").desc())
w2 = Window.partitionBy("promo_code").orderBy("starts_on", "id")
w3 = Window.partitionBy("channel").orderBy("starts_on", "id")
w4 = Window.partitionBy("channel").orderBy("starts_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("promo_code", "channel").orderBy(F.col("discount_pct_cume_dist").asc_nulls_first())
w6 = Window.partitionBy("channel").orderBy(F.col("ends_on").desc(), "id")
w7 = Window.partitionBy("promo_code", "channel").orderBy("discount_pct_cume_dist", "id").rowsBetween(-3, -1)
w8 = Window.partitionBy("channel").orderBy(F.col("discount_pct").desc())
w9 = Window.partitionBy("channel").orderBy("discount_pct_avg_3_prior", "id").rowsBetween(-6, Window.currentRow)
w10 = Window.partitionBy("channel").orderBy("rn", "id").rowsBetween(-5, Window.currentRow)
w11 = Window.partitionBy("channel").orderBy(F.col("discount_pct_percent_rank").desc_nulls_first(), "id")

enriched = (
    df_promotions
    .filter(F.col("discount_pct") >= 0.23)
    .select(
        "*",
        F.cume_dist().over(w).alias("discount_pct_cume_dist"),
        F.row_number().over(w2).alias("seq_no"),
        F.lag("discount_pct", 2).over(w3).alias("prev_discount_pct"),
        F.last("channel", ignorenulls=True).over(w4).alias("last_known_channel"),
    )
)

deduped = (
    enriched
    .withColumn("rn", F.rank().over(w5))
    .filter(F.col("rn") == 1)
)

enriched_promotions = (
    deduped
    .select(
        "id",
        "channel",
        "starts_on",
        "ends_on",
        "discount_pct",
        "seq_no",
        "last_known_channel",
        "rn",
        F.row_number().over(w6).alias("seq_no_2"),
        F.avg("discount_pct").over(w7).alias("discount_pct_avg_3_prior"),
    )
)

enriched_promotions_2 = (
    enriched_promotions
    .withColumn("discount_pct_percent_rank", F.percent_rank().over(w8))
    .withColumn("seq_no_2_avg_6", F.avg("seq_no_2").over(w9))
    .withColumn("seq_no_2_avg_5", F.avg("seq_no_2").over(w10))
)

cleaned = (
    enriched_promotions_2
    .filter(F.col("seq_no").between(5, 6))
    .select(
        "id",
        "channel",
        "starts_on",
        "ends_on",
        "discount_pct",
        "seq_no",
        "rn",
        "seq_no_2",
        "discount_pct_avg_3_prior",
        "discount_pct_percent_rank",
        "seq_no_2_avg_6",
        F.when(F.col("seq_no_2_avg_5") >= 8, "High").when(F.col("seq_no_2_avg_5") >= 4, "Medium").otherwise("Low").alias("seq_no_2_avg_5_band"),
    )
)

ranked = (
    cleaned
    .withColumn("rank_in_group", F.row_number().over(w11))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    ranked
    .select(
        "id",
        "starts_on",
        "ends_on",
        "discount_pct",
        "seq_no",
        "rn",
        "seq_no_2",
        "discount_pct_avg_3_prior",
        "discount_pct_percent_rank",
    )
    .orderBy(F.col("id").desc_nulls_last())
)
