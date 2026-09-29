from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("segment").orderBy("credit_limit", "id").rowsBetween(-2, -1)
w_running = Window.partitionBy("segment", "city").orderBy(F.col("credit_limit").desc_nulls_first())
w3 = Window.partitionBy("segment")
w4 = Window.orderBy("age", "id")
w_part = Window.partitionBy("country").orderBy("credit_limit", "id")

result = (
    df_customers
    .withColumn("credit_limit_avg_2_prior", F.avg("credit_limit").over(w_hist))
    .withColumn("credit_limit_cume_dist", F.cume_dist().over(w_running))
    .withColumn("corr_age_credit_limit", F.corr("age", "credit_limit").over(w3))
    .withColumn("prev_age", F.lag("age", 1).over(w4))
    .withColumn("credit_limit_bucket", F.ntile(4).over(w_part))
)
