from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("discount_pct")
w2 = Window.partitionBy("promo_code").orderBy("ends_on", "id")
w3 = Window.orderBy("ends_on", "id")
w4 = Window.partitionBy("promo_code", "channel").orderBy("ends_on", "id").rowsBetween(-5, -1)

result = (
    df_promotions
    .withColumn("discount_pct_percent_rank", F.percent_rank().over(w))
    .withColumn("row_seq", F.row_number().over(w2))
    .withColumn("prev_discount_pct", F.lag("discount_pct", 1).over(w3))
    .withColumn("discount_pct_avg_5_prior", F.avg("discount_pct").over(w4))
)
