from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_monthly_fee").desc())

result = (
    df_subscriptions
    .filter(F.col("customer_id") == 35)
    .groupBy(
        F.when(F.col("monthly_fee") >= 224, "high").when(F.col("monthly_fee") >= 112, "mid").otherwise("low").alias("monthly_fee_bucket"),
    )
    .agg(F.sum("monthly_fee").alias("total_monthly_fee"))
    .withColumn("rnk", F.dense_rank().over(w))
    .orderBy(F.col("monthly_fee_bucket").desc(), F.col("total_monthly_fee").desc())
)
