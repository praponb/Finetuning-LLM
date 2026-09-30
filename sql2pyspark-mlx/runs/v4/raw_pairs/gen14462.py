from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel")
w_running = Window.orderBy("discount_pct_int", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("promo_code", "channel").orderBy("discount_pct", "id").rowsBetween(-2, -1)
w4 = Window.partitionBy("channel").orderBy(F.col("discount_pct_int").desc(), "id")

base = (
    df_promotions
    .filter((F.col("starts_on") >= F.lit("2024-01-30").cast("date")) & F.month("ends_on").isin([2, 5, 9]))
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "discount_pct",
        F.col("discount_pct").cast("int").alias("discount_pct_int"),
        F.initcap(F.trim("channel")).alias("channel_title"),
        F.coalesce("channel", F.lit("N/A")).alias("resolved_channel"),
    )
)

result = (
    base
    .select(
        "*",
        F.count("discount_pct_int").over(w).alias("channel_count_discount_pct_int"),
        F.sum("discount_pct_int").over(w_running).alias("cumulative_discount_pct_int"),
        F.avg("discount_pct").over(w_ordered).alias("discount_pct_avg_2_prior"),
        F.ntile(4).over(w4).alias("discount_pct_int_bucket"),
    )
)
