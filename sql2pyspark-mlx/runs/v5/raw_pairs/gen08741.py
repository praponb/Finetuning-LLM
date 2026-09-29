from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("starts_on", "id").rowsBetween(-5, Window.currentRow)
w3 = Window.partitionBy("channel")

result = (
    df_promotions
    .select(
        "id",
        "channel",
        "promo_code",
        "ends_on",
        F.first("channel", ignorenulls=True).over(w).alias("first_known_channel"),
        F.avg("discount_pct").over(w2).alias("discount_pct_avg_5"),
        F.round((F.col("discount_pct") / F.sum("discount_pct").over(w3)), 4).alias("discount_pct_share"),
    )
)
