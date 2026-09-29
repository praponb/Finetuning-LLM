from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("ends_on", "id")
w2 = Window.partitionBy("channel", "promo_code").orderBy("starts_on", "id").rowsBetween(-5, -1)
w3 = Window.partitionBy("promo_code", "channel").orderBy("starts_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("channel")
w5 = Window.partitionBy("channel", "promo_code").orderBy("starts_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_promotions
    .withColumn("next_discount_pct", F.lead("discount_pct", 1).over(w))
    .withColumn("discount_pct_avg_5_prior", F.avg("discount_pct").over(w2))
    .withColumn("last_known_channel", F.last("channel", ignorenulls=True).over(w3))
    .withColumn("channel_avg_discount_pct", F.avg("discount_pct").over(w4))
    .withColumn("first_known_channel", F.first("channel", ignorenulls=True).over(w5))
)
