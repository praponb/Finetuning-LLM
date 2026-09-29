from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("promo_code").orderBy("starts_on_minus_6d", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_promotions
    .filter(F.col("channel").isNotNull() & (F.col("starts_on") >= "2025-10-01"))
    .withColumn("starts_on_minus_6d", F.date_sub("starts_on", 6))
    .withColumn("channel_upper", F.upper("channel"))
)

result = (
    filtered
    .select(
        "id",
        "starts_on",
        "ends_on",
        "channel_upper",
        F.first("channel", ignorenulls=True).over(w).alias("first_known_channel"),
        F.sum("discount_pct").over(w2).alias("cumulative_discount_pct"),
    )
)
