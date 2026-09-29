from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("start_date")

result = (
    df_subscriptions
    .select("id", "status", "start_date", F.dense_rank().over(w).alias("dense_rnk"))
)
