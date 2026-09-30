from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_windows = (
    df_promotions
    .select(
        "*",
        F.max("discount_pct").over(w).alias("running_max_discount_pct"),
        F.count("discount_pct").over(w).alias("running_count_discount_pct"),
        F.min("discount_pct").over(w).alias("running_min_discount_pct"),
        F.last("channel", ignorenulls=True).over(w).alias("last_known_channel"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "channel",
        "discount_pct",
        "running_count_discount_pct",
        "running_min_discount_pct",
        F.when(F.col("channel").rlike("^(mobile|web)$"), "GROUP_B").otherwise("CORE").alias("channel_zone"),
    )
    .orderBy(F.col("id").asc_nulls_last(), F.col("channel_zone").desc())
)
