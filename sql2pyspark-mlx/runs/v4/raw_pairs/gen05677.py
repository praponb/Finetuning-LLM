from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy("discount_pct", "id")
w2 = Window.partitionBy("promo_code").orderBy(F.col("discount_pct").desc(), "id")

result = (
    df_promotions
    .select(
        "*",
        F.lag("discount_pct", 1).over(w).alias("prev_discount_pct"),
        F.ntile(5).over(w2).alias("discount_pct_bucket"),
    )
)
