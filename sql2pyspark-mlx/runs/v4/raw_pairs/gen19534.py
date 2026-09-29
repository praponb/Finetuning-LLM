from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy("discount_pct")
w2 = Window.partitionBy("promo_code").orderBy("ends_on_month_end", "id")
w_recent = Window.partitionBy("promo_code").orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.orderBy(F.col("next_discount_pct").desc())
w5 = Window.partitionBy("promo_code").orderBy("ends_on_month_end", "id").rowsBetween(-3, Window.currentRow)
w6 = Window.partitionBy("promo_code").orderBy("ends_on_month_end", "id").rowsBetween(-6, -1)
w7 = Window.partitionBy("promo_code").orderBy("ends_on_month_end", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.orderBy(F.col("discount_pct_cume_dist").desc())
w9 = Window.partitionBy("promo_code")
w10 = Window.partitionBy("promo_code").orderBy(F.col("next_discount_pct_dense_rank").desc(), "id")

base = (
    df_promotions
    .filter((F.col("starts_on") >= F.lit("2024-03-13").cast("date")) & (F.col("discount_pct") <= 0.3))
    .select(
        "id",
        "promo_code",
        "discount_pct",
        F.last_day("ends_on").alias("ends_on_month_end"),
        F.coalesce("channel", F.lit("DIRECT")).alias("resolved_channel"),
    )
)

metrics = (
    base
    .withColumn("discount_pct_cume_dist", F.cume_dist().over(w))
    .withColumn("next_discount_pct", F.lead("discount_pct", 2).over(w2))
)

ranked = (
    metrics
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
)

scored = (
    ranked
    .select(
        "*",
        F.sum("next_discount_pct").over(w_recent).alias("cumulative_next_discount_pct"),
        F.dense_rank().over(w_running).alias("next_discount_pct_dense_rank"),
        F.avg("next_discount_pct").over(w5).alias("next_discount_pct_avg_3"),
    )
)

with_windows = (
    scored
    .withColumn("cumulative_next_discount_pct_avg_6_prior", F.avg("cumulative_next_discount_pct").over(w6))
    .withColumn("first_known_cumulative_next_discount_pct", F.first("cumulative_next_discount_pct", ignorenulls=True).over(w7))
    .withColumn("discount_pct_cume_dist_percent_rank", F.percent_rank().over(w8))
    .withColumn("promo_code_max_discount_pct", F.max("discount_pct").over(w9))
    .withColumn("row_seq", F.row_number().over(w10))
)

result = (
    with_windows
    .select(
        "id",
        "promo_code",
        "ends_on_month_end",
        "discount_pct_cume_dist",
        "next_discount_pct",
        "latest_rank",
        "cumulative_next_discount_pct",
        "next_discount_pct_avg_3",
        "cumulative_next_discount_pct_avg_6_prior",
        F.when(F.col("promo_code").rlike("^(SPRING10|WELCOME5)$"), "EXTENDED").otherwise("GROUP_B").alias("promo_code_zone"),
    )
    .orderBy(F.col("discount_pct_cume_dist").desc_nulls_first())
)
