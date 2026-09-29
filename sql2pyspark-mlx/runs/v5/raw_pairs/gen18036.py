from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.orderBy("starts_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("promo_code", "channel").orderBy(F.col("starts_on_day").asc_nulls_first())
w4 = Window.partitionBy("channel")
w5 = Window.partitionBy("promo_code").orderBy(F.col("starts_on_day").desc(), "id")

cleaned = (
    df_promotions
    .filter(F.col("channel").isNotNull())
    .withColumn("starts_on_day", F.dayofmonth("starts_on"))
    .withColumn("resolved_channel", F.coalesce("channel", F.lit("N/A")))
)

result = (
    cleaned
    .select(
        "id",
        "starts_on",
        "ends_on",
        "discount_pct",
        "resolved_channel",
        F.first("channel", ignorenulls=True).over(w_running).alias("first_known_channel"),
        F.first("starts_on_day", ignorenulls=True).over(w2).alias("first_known_starts_on_day"),
        F.cume_dist().over(w3).alias("starts_on_day_cume_dist"),
        F.round((F.col("discount_pct") / F.sum("discount_pct").over(w4)), 4).alias("discount_pct_share"),
        F.ntile(10).over(w5).alias("starts_on_day_bucket"),
    )
)
