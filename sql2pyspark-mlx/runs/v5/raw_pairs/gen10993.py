from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("monthly_fee_bucket")

result = (
    df_subscriptions
    .filter(F.col("plan") == "basic")
    .groupBy(
        F.when(F.col("monthly_fee") >= 227, "high").when(F.col("monthly_fee") >= 115, "mid").otherwise("low").alias("monthly_fee_bucket"),
    )
    .agg(F.sum("monthly_fee").alias("total_monthly_fee"))
    .withColumn("cumulative_monthly_fee", F.sum("total_monthly_fee").over(w))
)
