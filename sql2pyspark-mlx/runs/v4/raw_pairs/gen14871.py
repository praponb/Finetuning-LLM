from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_monthly_fee").desc())

result = (
    df_subscriptions
    .filter(F.col("start_date").between("2024-06-01", "2025-08-04"))
    .groupBy("status")
    .agg(F.sum("monthly_fee").alias("total_monthly_fee"))
    .withColumn("rnk", F.rank().over(w))
)
