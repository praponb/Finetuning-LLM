from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("monthly_fee")

result = (
    df_subscriptions
    .withColumn("pos", F.rank().over(w))
    .filter(F.col("pos") == 1)
    .select("id", "status", "monthly_fee", "start_date")
    .orderBy(F.col("id").desc(), F.col("monthly_fee").desc())
)
