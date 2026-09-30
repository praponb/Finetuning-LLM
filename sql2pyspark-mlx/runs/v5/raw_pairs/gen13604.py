from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("channel").orderBy("starts_on", "id")
w2 = Window.partitionBy("channel").orderBy("ends_on", "id")

result = (
    df_promotions
    .select(
        "id",
        "channel",
        "promo_code",
        F.lag("discount_pct").over(w_recent).alias("prev_discount_pct"),
        F.lead("discount_pct").over(w_recent).alias("next_discount_pct"),
        F.row_number().over(w_recent).alias("seq_no"),
        F.row_number().over(w2).alias("seq_no_2"),
    )
)
