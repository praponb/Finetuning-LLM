from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.orderBy("ends_on", "id")
w_part = Window.orderBy(F.col("discount_pct").desc(), "id")
w_recent = Window.partitionBy("promo_code", "channel")
w4 = Window.partitionBy("channel").orderBy("ends_on", "id").rowsBetween(-3, Window.currentRow)
w_grp = Window.orderBy("starts_on", "id").rowsBetween(-2, Window.currentRow)

result = (
    df_promotions
    .withColumn("seq_no", F.row_number().over(w_hist))
    .withColumn("discount_pct_bucket", F.ntile(4).over(w_part))
    .withColumn("promo_code_stddev_samp_discount_pct", F.stddev_samp("discount_pct").over(w_recent))
    .withColumn("discount_pct_avg_3", F.avg("discount_pct").over(w4))
    .withColumn("discount_pct_avg_2", F.avg("discount_pct").over(w_grp))
)
