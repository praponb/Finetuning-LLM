from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("channel").orderBy(F.col("starts_on").desc(), "id")

filtered = (
    df_promotions
    .filter((~F.col("channel").eqNullSafe(F.lit("partner"))) & F.col("channel").isNotNull())
    .select(
        "id",
        "channel",
        "starts_on",
        "discount_pct",
        F.when(F.col("discount_pct") >= 0.28, "High").when(F.col("discount_pct") >= 0.18, "Medium").otherwise("Low").alias("discount_pct_tier"),
        F.initcap(F.trim("channel")).alias("channel_title"),
        F.coalesce("channel", F.lit("N/A")).alias("channel_filled"),
    )
)

result = (
    filtered
    .filter(F.col("channel").isin(["web", "partner", "mobile"]))
    .select(
        "id",
        "channel",
        "starts_on",
        "discount_pct",
        "channel_title",
        "channel_filled",
        F.avg("discount_pct").over(w).alias("running_avg_discount_pct"),
        F.min("discount_pct").over(w).alias("running_min_discount_pct"),
        F.sum("discount_pct").over(w).alias("running_sum_discount_pct"),
        F.row_number().over(w2).alias("row_seq"),
    )
)
