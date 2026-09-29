from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("starts_on", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("channel").orderBy("discount_pct", "id")

result = (
    df_promotions
    .select(
        "id",
        "channel",
        "starts_on",
        F.avg("discount_pct").over(w).alias("discount_pct_avg_3"),
        F.ntile(4).over(w2).alias("discount_pct_bucket"),
    )
)
