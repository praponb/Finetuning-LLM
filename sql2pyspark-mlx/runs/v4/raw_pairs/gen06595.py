from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status")

result = (
    df_subscriptions
    .withColumn("max_monthly_fee_by_status", F.max("monthly_fee").over(w))
    .filter(F.col("monthly_fee") == F.col("max_monthly_fee_by_status"))
    .select("id", "status", "monthly_fee")
)
