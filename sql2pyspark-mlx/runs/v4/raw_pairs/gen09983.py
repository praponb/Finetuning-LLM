from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code", "channel")
w2 = Window.partitionBy("promo_code").orderBy(F.col("discount_pct").desc(), "id")
w3 = Window.partitionBy("channel").orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_windows = (
    df_promotions
    .select(
        "*",
        F.min("discount_pct").over(w).alias("promo_code_min_discount_pct"),
        F.row_number().over(w2).alias("event_seq"),
        F.first("channel", ignorenulls=True).over(w3).alias("first_known_channel"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "channel",
        "ends_on",
        "discount_pct",
        "promo_code_min_discount_pct",
        "event_seq",
        "first_known_channel",
        F.when(F.col("channel").rlike("^(partner|web)$"), "OTHER_ZONE").otherwise("CORE").alias("channel_zone"),
    )
)
