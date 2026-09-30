from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("channel").orderBy("discount_pct", "id")
w3 = Window.partitionBy("channel")
w4 = Window.partitionBy("channel").orderBy("days_since_ends_on", "id").rowsBetween(-3, -1)

cleaned = (
    df_promotions
    .filter(F.col("channel").isNotNull())
    .select(
        "id",
        "channel",
        "discount_pct",
        F.datediff(F.current_date(), "ends_on").alias("days_since_ends_on"),
    )
)

result = (
    cleaned
    .filter(F.col("days_since_ends_on") >= 61)
    .select(
        "id",
        "discount_pct",
        "days_since_ends_on",
        F.sum("days_since_ends_on").over(w).alias("cumulative_days_since_ends_on"),
        F.lag("days_since_ends_on", 1).over(w2).alias("prev_days_since_ends_on"),
        F.round((F.col("discount_pct") / F.sum("discount_pct").over(w3)), 4).alias("discount_pct_share"),
        F.lead("days_since_ends_on", 1, 0).over(w2).alias("next_days_since_ends_on"),
        F.avg("discount_pct").over(w4).alias("discount_pct_avg_3_prior"),
    )
)
