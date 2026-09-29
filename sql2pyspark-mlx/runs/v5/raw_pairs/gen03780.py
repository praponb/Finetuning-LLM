from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("starts_on", "id")
w2 = Window.partitionBy("channel")
w3 = Window.partitionBy("promo_code", "channel").orderBy("discount_pct", "id")
w_recent = Window.partitionBy("channel", "promo_code").orderBy("discount_pct", "id")
w5 = Window.partitionBy("promo_code")
w6 = Window.partitionBy("channel", "promo_code").orderBy(F.col("discount_pct").desc(), "id")
w7 = Window.orderBy("discount_pct_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("promo_code").orderBy("channel_avg_discount_pct", "id")
w9 = Window.partitionBy("promo_code").orderBy("seq_no_2", "id").rowsBetween(-3, Window.currentRow)
w10 = Window.partitionBy("promo_code").orderBy(F.col("promo_code_avg_discount_pct_bucket").desc(), "id")
w11 = Window.partitionBy("promo_code").orderBy("seq_no_2", "id").rowsBetween(-5, -1)

metrics = (
    df_promotions
    .withColumn("prev_discount_pct", F.lag("discount_pct", 1).over(w))
    .withColumn("channel_avg_discount_pct", F.avg("discount_pct").over(w2))
    .withColumn("discount_pct_bucket", F.ntile(4).over(w3))
)

metrics_promotions = (
    metrics
    .select(
        "id",
        "promo_code",
        "discount_pct",
        F.row_number().over(w_recent).alias("seq_no"),
        F.avg("discount_pct_bucket").over(w5).alias("promo_code_avg_discount_pct_bucket"),
        F.row_number().over(w6).alias("seq_no_2"),
        F.sum("channel_avg_discount_pct").over(w7).alias("cumulative_channel_avg_discount_pct"),
        F.ntile(10).over(w8).alias("channel_avg_discount_pct_bucket"),
    )
)

enriched = (
    metrics_promotions
    .select(
        "id",
        "promo_code",
        "promo_code_avg_discount_pct_bucket",
        "cumulative_channel_avg_discount_pct",
        "channel_avg_discount_pct_bucket",
        F.avg("cumulative_channel_avg_discount_pct").over(w9).alias("cumulative_channel_avg_discount_pct_avg_3"),
        F.ntile(10).over(w10).alias("promo_code_avg_discount_pct_bucket_bucket"),
        F.avg("discount_pct").over(w11).alias("discount_pct_avg_5_prior"),
    )
)

cleaned = (
    enriched
    .filter(F.col("channel_avg_discount_pct_bucket").between(3, 5))
    .withColumn("cumulative_channel_avg_discount_pct_avg_3_whole", F.floor("cumulative_channel_avg_discount_pct_avg_3"))
)

cleaned_promotions = (
    cleaned
    .filter(F.col("discount_pct_avg_5_prior") == 0)
    .select(
        "*",
        F.greatest("cumulative_channel_avg_discount_pct", "cumulative_channel_avg_discount_pct_avg_3_whole").alias("greatest_cumulative_channel_avg_discount_pct_cumulative_channel_avg_discount_pct_avg_3_whole"),
        F.when(F.col("promo_code") == "WELCOME5", 1).otherwise(0).alias("is_welcome5"),
    )
)

result = (
    cleaned_promotions
    .select(
        "id",
        "promo_code",
        "promo_code_avg_discount_pct_bucket",
        "cumulative_channel_avg_discount_pct",
        "channel_avg_discount_pct_bucket",
        "cumulative_channel_avg_discount_pct_avg_3",
        "promo_code_avg_discount_pct_bucket_bucket",
        "discount_pct_avg_5_prior",
        "greatest_cumulative_channel_avg_discount_pct_cumulative_channel_avg_discount_pct_avg_3_whole",
        F.when(F.col("promo_code").rlike("^(WELCOME5|VIP)$"), "GROUP_B").when(F.col("promo_code").rlike("^(SPRING10|BUNDLE)$"), "PRIORITY").otherwise("GROUP_A").alias("promo_code_zone"),
    )
)
