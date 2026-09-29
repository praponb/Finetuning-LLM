from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status")

result = (
    df_subscriptions
    .withColumn("avg_monthly_fee_by_status", F.avg("monthly_fee").over(w))
    .filter(F.col("monthly_fee") < F.col("avg_monthly_fee_by_status"))
    .select("id", "status", "monthly_fee", "customer_id")
    .orderBy(F.col("id").desc())
)
