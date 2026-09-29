from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.orderBy("discount_pct", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.orderBy("ends_on", "id").rowsBetween(-6, Window.currentRow)

valid = (
    df_promotions
    .filter(F.col("channel").isNotNull())
    .select(
        "*",
        F.date_add("starts_on", 69).alias("starts_on_plus_69d"),
        F.coalesce("channel", F.lit("DIRECT")).alias("channel_filled"),
    )
)

result = (
    valid
    .withColumn("discount_pct_avg_2", F.avg("discount_pct").over(w_running))
    .withColumn("discount_pct_avg_6", F.avg("discount_pct").over(w2))
)
