from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status")

result = (
    df_subscriptions
    .withColumn("min_monthly_fee_by_status", F.min("monthly_fee").over(w))
    .filter(F.col("monthly_fee") == F.col("min_monthly_fee_by_status"))
    .select("id", "status", "monthly_fee", "start_date")
    .orderBy(F.col("monthly_fee").desc())
)
