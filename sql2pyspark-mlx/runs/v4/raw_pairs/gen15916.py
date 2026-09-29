from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("discount_pct", "id").rowsBetween(-5, -1)
w2 = Window.partitionBy("channel").orderBy("starts_on", "id").rowsBetween(-5, Window.currentRow)

result = (
    df_promotions
    .withColumn("discount_pct_avg_5_prior", F.avg("discount_pct").over(w))
    .withColumn("discount_pct_avg_5", F.avg("discount_pct").over(w2))
)
