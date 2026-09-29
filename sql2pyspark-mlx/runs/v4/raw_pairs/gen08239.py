from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy("ends_on", "id")
w2 = Window.partitionBy("channel").orderBy("ends_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("channel").orderBy("ends_on", "id").rowsBetween(-6, -1)
w4 = Window.partitionBy("promo_code").orderBy("row_num", "id")
w5 = Window.partitionBy("channel").orderBy(F.col("discount_pct_adjusted").asc_nulls_last())
w6 = Window.orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_promotions
    .filter(F.col("discount_pct") <= 0.15)
    .withColumn("discount_pct_adjusted", F.round((F.col("discount_pct") * 1.07), 2))
)

ranked = (
    base
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

with_windows = (
    ranked
    .select(
        "*",
        F.sum("discount_pct_adjusted").over(w2).alias("cumulative_discount_pct_adjusted"),
        F.avg("discount_pct").over(w3).alias("discount_pct_avg_6_prior"),
        F.row_number().over(w4).alias("event_seq"),
        F.percent_rank().over(w5).alias("discount_pct_adjusted_percent_rank"),
        F.first("channel", ignorenulls=True).over(w6).alias("first_known_channel"),
    )
)

filtered = (
    with_windows
    .filter((F.col("ends_on") >= F.lit("2023-10-21").cast("date")) & F.col("channel").isNotNull())
    .withColumn("discount_pct_scaled", F.round((F.col("discount_pct") * 1.15), 2))
    .withColumn("discount_pct_adjusted_filled", F.coalesce("discount_pct_adjusted", F.lit(0)))
)

result = (
    filtered
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        "row_num",
        "event_seq",
        "discount_pct_adjusted_percent_rank",
        F.when(F.col("promo_code").rlike("^(SPRING10|WELCOME5)$"), "GROUP_B").otherwise("PRIORITY").alias("promo_code_zone"),
    )
)
