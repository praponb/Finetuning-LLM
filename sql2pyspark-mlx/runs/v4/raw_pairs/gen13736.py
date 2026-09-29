from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("channel", "promo_code").orderBy(F.col("discount_pct").desc(), "id")
w_part_2 = Window.orderBy(F.col("ends_on").desc(), "id")
w3 = Window.partitionBy("channel")
w4 = Window.partitionBy("promo_code")

result = (
    df_promotions
    .select(
        "*",
        F.ntile(4).over(w_part).alias("discount_pct_bucket"),
        F.row_number().over(w_part_2).alias("seq_no"),
        F.max("discount_pct").over(w3).alias("channel_max_discount_pct"),
        F.round((F.col("discount_pct") / F.sum("discount_pct").over(w4)), 4).alias("discount_pct_share"),
    )
)
