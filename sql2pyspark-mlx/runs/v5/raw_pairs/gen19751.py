from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy()

result = (
    df_subscriptions
    .filter(F.col("customer_id") == 25)
    .groupBy(F.month("start_date").alias("start_month"))
    .agg(F.sum("monthly_fee").alias("total_monthly_fee"))
    .withColumn("pct_of_total", F.round(((F.col("total_monthly_fee") * 100) / F.sum("total_monthly_fee").over(w)), 2))
)
