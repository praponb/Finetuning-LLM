from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel", "promo_code").orderBy(F.col("discount_pct").desc(), "id")
w2 = Window.partitionBy("channel").orderBy("discount_pct", "id")

result = (
    df_promotions
    .filter(F.col("discount_pct") > 0.16)
    .select(
        "id",
        "discount_pct",
        F.row_number().over(w).alias("row_seq"),
        F.lag("discount_pct", 1, 0).over(w2).alias("prev_discount_pct"),
    )
)
