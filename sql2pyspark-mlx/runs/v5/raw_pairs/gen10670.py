from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel", "promo_code")
w_running = Window.orderBy("starts_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_promotions
    .select(
        "id",
        "starts_on",
        "discount_pct",
        F.avg("discount_pct").over(w).alias("channel_avg_discount_pct"),
        F.sum("discount_pct").over(w_running).alias("running_discount_pct"),
    )
)
