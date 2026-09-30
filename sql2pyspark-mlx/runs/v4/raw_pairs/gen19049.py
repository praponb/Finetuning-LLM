from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code")
w2 = Window.partitionBy("channel", "promo_code").orderBy(F.col("discount_pct").desc())

result = (
    df_promotions
    .select(
        "*",
        F.stddev_samp("discount_pct").over(w).alias("promo_code_stddev_samp_discount_pct"),
        F.rank().over(w2).alias("discount_pct_rank"),
    )
)
