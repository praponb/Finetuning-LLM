from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("promo_code").orderBy("ends_on", "id")
w2 = Window.partitionBy("promo_code", "channel").orderBy("starts_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_promotions
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        F.lead("discount_pct").over(w_part).alias("next_discount_pct"),
        F.lag("discount_pct").over(w_part).alias("prev_discount_pct"),
        F.row_number().over(w_part).alias("event_seq"),
        F.sum("discount_pct").over(w2).alias("cumulative_discount_pct"),
    )
)
