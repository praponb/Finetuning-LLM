from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("monthly_fee").desc())
w2 = Window.partitionBy("plan")
w3 = Window.partitionBy("plan").orderBy(F.col("monthly_fee").desc_nulls_first())

result = (
    df_subscriptions
    .withColumn("monthly_fee_cume_dist", F.cume_dist().over(w))
    .withColumn("monthly_fee_share", F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w2)), 4))
    .withColumn("monthly_fee_percent_rank", F.percent_rank().over(w3))
)
