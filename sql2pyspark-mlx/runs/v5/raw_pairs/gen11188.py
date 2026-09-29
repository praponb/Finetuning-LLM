from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("monthly_fee").desc(), "id")

result = (
    df_subscriptions
    .filter(F.col("monthly_fee") > 140)
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") <= 3)
    .select("id", "plan", "monthly_fee")
    .orderBy("plan")
)
