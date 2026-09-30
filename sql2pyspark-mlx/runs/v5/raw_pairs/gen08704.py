from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("monthly_fee").desc())

result = (
    df_subscriptions
    .filter(F.col("start_date") <= "2024-08-04")
    .withColumn("row_num", F.dense_rank().over(w))
    .filter(F.col("row_num") <= 2)
    .orderBy(F.col("monthly_fee").desc())
)
