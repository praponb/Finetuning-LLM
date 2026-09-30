from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("discount_pct").desc(), "id")
w2 = Window.partitionBy("channel").orderBy("discount_pct", "id")
w3 = Window.partitionBy("promo_code").orderBy("ends_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_promotions
    .filter(F.year("starts_on") >= 2025)
    .select(
        "id",
        "ends_on",
        "discount_pct",
        F.ntile(10).over(w).alias("discount_pct_bucket"),
        F.lag("discount_pct", 1).over(w2).alias("prev_discount_pct"),
        F.last("channel", ignorenulls=True).over(w3).alias("last_known_channel"),
    )
)
