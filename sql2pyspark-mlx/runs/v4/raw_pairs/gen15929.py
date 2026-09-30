from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code", "channel")
w2 = Window.partitionBy("channel", "promo_code").orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("promo_code")
w4 = Window.partitionBy("channel").orderBy("ends_on", "id").rowsBetween(-6, -1)
w5 = Window.partitionBy("channel")
w6 = Window.orderBy("promo_code_avg_discount_pct", "id")
w7 = Window.partitionBy("channel").orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("channel").orderBy("promo_code_avg_discount_pct", "id")

metrics = (
    df_promotions
    .select(
        "*",
        F.avg("discount_pct").over(w).alias("promo_code_avg_discount_pct"),
        F.sum("discount_pct").over(w2).alias("cumulative_discount_pct"),
        F.round((F.col("discount_pct") / F.sum("discount_pct").over(w3)), 4).alias("discount_pct_share"),
        F.avg("discount_pct").over(w4).alias("discount_pct_avg_6_prior"),
        F.round((F.col("discount_pct") / F.sum("discount_pct").over(w5)), 4).alias("discount_pct_share_2"),
    )
)

cleaned = (
    metrics
    .filter((F.col("ends_on") >= F.lit("2023-06-11").cast("date")) & F.col("channel").isNotNull() & F.col("discount_pct_share_2").between(0.42, 0.56))
    .select(
        "id",
        "channel",
        "starts_on",
        "discount_pct",
        "promo_code_avg_discount_pct",
        "cumulative_discount_pct",
        "discount_pct_avg_6_prior",
        "discount_pct_share_2",
        F.coalesce("discount_pct_share", F.lit(0)).alias("discount_pct_share_filled"),
        F.coalesce("channel", F.lit("DIRECT")).alias("channel_clean"),
    )
)

enriched = (
    cleaned
    .select(
        "*",
        F.lead("discount_pct", 1, 0).over(w6).alias("next_discount_pct"),
        F.sum("discount_pct_share_2").over(w7).alias("cumulative_discount_pct_share_2"),
    )
)

ranked = (
    enriched
    .withColumn("row_num", F.row_number().over(w8))
    .filter(F.col("row_num") == 1)
)

base = (
    ranked
    .filter(F.col("starts_on") > F.lit("2023-06-06").cast("date"))
    .select(
        "*",
        F.ceil("cumulative_discount_pct_share_2").alias("cumulative_discount_pct_share_2_whole"),
        F.coalesce("channel", F.lit("NONE")).alias("channel_clean_2"),
    )
)

result = (
    base
    .select(
        "id",
        "channel",
        "promo_code_avg_discount_pct",
        "cumulative_discount_pct",
        "discount_pct_avg_6_prior",
        "discount_pct_share_2",
        "discount_pct_share_filled",
        "next_discount_pct",
        "cumulative_discount_pct_share_2",
        F.when(F.col("channel").rlike("^(store|web)$"), "GROUP_B").when(F.col("channel").rlike("^(mobile|partner)$"), "CORE").otherwise("EXTENDED").alias("channel_zone"),
    )
    .orderBy(F.col("channel_zone").desc(), "cumulative_discount_pct_share_2")
)
