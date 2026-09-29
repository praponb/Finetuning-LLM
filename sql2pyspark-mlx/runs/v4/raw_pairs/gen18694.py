from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("start_date").desc())

result = (
    df_subscriptions
    .filter(F.year("start_date") == 2025)
    .withColumn("row_num", F.dense_rank().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "customer_id", "start_date")
    .orderBy("id")
)
