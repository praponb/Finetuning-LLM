from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("customer_id")
w2 = Window.orderBy(F.col("monthly_fee").desc_nulls_first())
w3 = Window.partitionBy("status", "customer_id").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_subscriptions
    .withColumn("customer_id_min_monthly_fee", F.min("monthly_fee").over(w_grp))
    .withColumn("monthly_fee_percent_rank", F.percent_rank().over(w2))
    .withColumn("cumulative_monthly_fee", F.sum("monthly_fee").over(w3))
)
