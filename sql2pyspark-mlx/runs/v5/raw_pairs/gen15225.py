from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy("discount_pct", "id")
w2 = Window.partitionBy("channel", "promo_code").orderBy("starts_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_promotions
    .filter(F.col("ends_on") <= "2025-05-01")
    .select(
        "id",
        "channel",
        "promo_code",
        "ends_on",
        F.lag("discount_pct").over(w).alias("prev_discount_pct"),
        F.row_number().over(w).alias("seq_no"),
        F.sum("discount_pct").over(w2).alias("running_discount_pct"),
    )
)
