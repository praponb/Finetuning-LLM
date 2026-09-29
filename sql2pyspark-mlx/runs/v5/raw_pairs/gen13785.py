from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code", "channel").orderBy("ends_on", "id")
w2 = Window.partitionBy("channel", "promo_code")

enriched = (
    df_promotions
    .filter(F.col("starts_on") < "2025-05-01")
    .select(
        "id",
        "starts_on",
        "discount_pct",
        F.lead("discount_pct").over(w).alias("next_discount_pct"),
        F.lag("discount_pct").over(w).alias("prev_discount_pct"),
        F.row_number().over(w).alias("event_seq"),
        F.sum("discount_pct").over(w2).alias("channel_sum_discount_pct"),
    )
)

result = (
    enriched
    .select(
        "id",
        "discount_pct",
        "next_discount_pct",
        "prev_discount_pct",
        "event_seq",
        "channel_sum_discount_pct",
    )
    .orderBy(F.col("discount_pct").asc_nulls_first())
)
