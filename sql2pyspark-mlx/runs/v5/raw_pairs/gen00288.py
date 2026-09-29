from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code")
w_part = Window.partitionBy("channel").orderBy("ends_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_promotions
    .filter(F.col("ends_on") < "2024-02-01")
    .select(
        "id",
        "promo_code",
        F.round((F.col("discount_pct") / F.sum("discount_pct").over(w)), 4).alias("discount_pct_share"),
        F.first("channel", ignorenulls=True).over(w_part).alias("first_known_channel"),
    )
)
