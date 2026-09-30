from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan")
w2 = Window.partitionBy("customer_id")
w3 = Window.partitionBy("status").orderBy(F.col("monthly_fee").asc_nulls_first())
w4 = Window.partitionBy("customer_id").orderBy(F.col("start_date").desc(), "id")

result = (
    df_subscriptions
    .withColumn("plan_min_monthly_fee", F.min("monthly_fee").over(w))
    .withColumn("monthly_fee_share", F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w2)), 4))
    .withColumn("monthly_fee_cume_dist", F.cume_dist().over(w3))
    .withColumn("seq_no", F.row_number().over(w4))
)
