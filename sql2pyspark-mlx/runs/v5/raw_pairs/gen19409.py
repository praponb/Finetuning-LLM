from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy("ends_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("promo_code").orderBy("starts_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy(F.col("starts_on").desc(), "id")
w4 = Window.partitionBy("channel").orderBy(F.col("discount_pct").desc())
w5 = Window.partitionBy("channel", "promo_code")

with_windows = (
    df_promotions
    .filter(F.col("promo_code") != "BUNDLE")
    .select(
        "*",
        F.last("channel", ignorenulls=True).over(w).alias("last_known_channel"),
        F.sum("discount_pct").over(w2).alias("running_discount_pct"),
        F.row_number().over(w3).alias("row_seq"),
        F.cume_dist().over(w4).alias("discount_pct_cume_dist"),
        F.round((F.col("discount_pct") / F.sum("discount_pct").over(w5)), 4).alias("discount_pct_share"),
    )
)

result = (
    with_windows
    .withColumn("activity_month", F.date_trunc("MONTH", "ends_on"))
    .withColumn("discount_pct_share_tier", F.when(F.col("discount_pct_share") >= 0.67, "LARGE").otherwise("SMALL"))
    .withColumn("year_start", F.date_trunc("YEAR", "starts_on"))
    .cube("activity_month", "discount_pct_share_tier", "year_start")
    .agg(
        F.countDistinct("channel").alias("distinct_channels"),
        F.count_if(F.col("promo_code") == "SPRING10").alias("spring10_events"),
        F.count("*").alias("cnt"),
        F.countDistinct("channel").alias("distinct_channels_2"),
        F.grouping_id().alias("grouping_level"),
    )
)
