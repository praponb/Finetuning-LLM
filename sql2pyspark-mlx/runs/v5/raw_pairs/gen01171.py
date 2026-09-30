from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("discount_pct").desc())
w2 = Window.partitionBy("channel").orderBy("ends_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

metrics = (
    df_promotions
    .filter(F.col("ends_on") >= "2024-04-01")
    .select(
        "id",
        "channel",
        "starts_on",
        "discount_pct",
        F.percent_rank().over(w).alias("discount_pct_percent_rank"),
        F.first("channel", ignorenulls=True).over(w2).alias("first_known_channel"),
    )
)

result = (
    metrics
    .select(
        "id",
        "starts_on",
        "discount_pct",
        "discount_pct_percent_rank",
        "first_known_channel",
    )
    .orderBy(F.col("discount_pct_percent_rank").desc_nulls_last())
)
