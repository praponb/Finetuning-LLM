from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("discount_pct").desc(), "id")
w2 = Window.partitionBy("promo_code").orderBy(F.col("discount_pct").desc(), "id")
w3 = Window.partitionBy("channel", "promo_code").orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("channel").orderBy("starts_on", "id")

result = (
    df_promotions
    .select(
        "*",
        F.row_number().over(w).alias("event_seq"),
        F.row_number().over(w2).alias("seq_no"),
        F.last("channel", ignorenulls=True).over(w3).alias("last_known_channel"),
        F.lead("discount_pct", 1).over(w4).alias("next_discount_pct"),
    )
)
