from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code", "channel").orderBy(F.col("discount_pct").desc(), "id")
w2 = Window.partitionBy("channel").orderBy("ends_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("promo_code").orderBy("ends_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scored = (
    df_promotions
    .select(
        "*",
        F.ntile(5).over(w).alias("discount_pct_bucket"),
        F.last("channel", ignorenulls=True).over(w2).alias("last_known_channel"),
        F.first("channel", ignorenulls=True).over(w_running).alias("first_known_channel"),
    )
)

result = (
    scored
    .select(
        "id",
        "channel",
        "starts_on",
        "ends_on",
        "discount_pct_bucket",
        "last_known_channel",
        "first_known_channel",
    )
    .orderBy(F.col("discount_pct_bucket").desc())
)
