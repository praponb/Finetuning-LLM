from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("ends_on", "id").rowsBetween(-6, Window.currentRow)
w_running = Window.partitionBy("channel").orderBy("starts_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("promo_code").orderBy(F.col("discount_pct").desc())

result = (
    df_promotions
    .select(
        "id",
        "channel",
        "ends_on",
        F.avg("discount_pct").over(w).alias("discount_pct_avg_6"),
        F.sum("discount_pct").over(w_running).alias("cumulative_discount_pct"),
        F.rank().over(w3).alias("discount_pct_rank"),
    )
)
