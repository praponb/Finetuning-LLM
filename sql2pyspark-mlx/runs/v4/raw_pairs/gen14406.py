from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_monthly_fee").desc())

result = (
    df_subscriptions
    .filter(F.year("end_date") == 2025)
    .groupBy("plan")
    .agg(F.sum("monthly_fee").alias("total_monthly_fee"))
    .withColumn("rnk", F.dense_rank().over(w))
)
