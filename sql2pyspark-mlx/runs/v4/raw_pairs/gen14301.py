from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel", "promo_code").orderBy("discount_pct")
w2 = Window.partitionBy("channel").orderBy("discount_pct")

result = (
    df_promotions
    .select(
        "*",
        F.rank().over(w).alias("discount_pct_rank"),
        F.rank().over(w2).alias("discount_pct_rank_2"),
    )
)
