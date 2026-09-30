from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("channel").orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("promo_code", "channel").orderBy("starts_on", "id").rowsBetween(-5, Window.currentRow)

scored = (
    df_promotions
    .select(
        "*",
        F.sum("discount_pct").over(w_hist).alias("running_discount_pct"),
        F.avg("discount_pct").over(w_running).alias("discount_pct_avg_5"),
    )
)

result = (
    scored
    .select(
        "id",
        "channel",
        "starts_on",
        "ends_on",
        "discount_pct",
        "running_discount_pct",
        "discount_pct_avg_5",
    )
    .orderBy("running_discount_pct", "ends_on")
)
