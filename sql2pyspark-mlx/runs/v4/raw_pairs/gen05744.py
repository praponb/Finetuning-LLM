from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("segment")
w_part_2 = Window.partitionBy("segment", "country")
w3 = Window.partitionBy("city")
w4 = Window.partitionBy("country").orderBy(F.col("credit_limit").desc(), "id")
w5 = Window.partitionBy("country")

result = (
    df_customers
    .withColumn("segment_stddev_samp_credit_limit", F.stddev_samp("credit_limit").over(w_part))
    .withColumn("segment_stddev_samp_age", F.stddev_samp("age").over(w_part_2))
    .withColumn("city_min_age", F.min("age").over(w3))
    .withColumn("credit_limit_bucket", F.ntile(3).over(w4))
    .withColumn("credit_limit_share", F.round((F.col("credit_limit") / F.sum("credit_limit").over(w5)), 4))
)
