from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("monthly_fee", "id")

result = (
    df_subscriptions
    .filter(F.col("monthly_fee") <= 210)
    .withColumn("rnk", F.row_number().over(w))
    .filter(F.col("rnk") <= 5)
    .select("id", "plan", "monthly_fee")
    .orderBy("monthly_fee")
)
