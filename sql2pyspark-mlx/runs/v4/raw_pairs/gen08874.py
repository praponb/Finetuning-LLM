from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("promo_code", "channel").orderBy(F.col("discount_pct").desc())
w2 = Window.orderBy("discount_pct", "id")
w_part = Window.partitionBy("channel").orderBy("discount_pct")

result = (
    df_promotions
    .select(
        "*",
        F.percent_rank().over(w_grp).alias("discount_pct_percent_rank"),
        F.ntile(3).over(w2).alias("discount_pct_bucket"),
        F.cume_dist().over(w_part).alias("discount_pct_cume_dist"),
    )
)
