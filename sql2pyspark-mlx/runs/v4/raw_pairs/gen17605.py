from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("customer_id").orderBy(F.col("monthly_fee").desc(), "id")
w_running = Window.partitionBy("plan").orderBy("monthly_fee", "id")
w3 = Window.orderBy("monthly_fee", "id")

result = (
    df_subscriptions
    .withColumn("monthly_fee_bucket", F.ntile(10).over(w_part))
    .withColumn("monthly_fee_bucket_2", F.ntile(5).over(w_running))
    .withColumn("monthly_fee_bucket_3", F.ntile(4).over(w3))
)
