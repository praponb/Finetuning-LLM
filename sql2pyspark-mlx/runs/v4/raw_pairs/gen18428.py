from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy("ends_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("promo_code", "channel")

result = (
    df_promotions
    .select(
        "*",
        F.sum("discount_pct").over(w).alias("cumulative_discount_pct"),
        F.min("discount_pct").over(w_recent).alias("promo_code_min_discount_pct"),
    )
)
