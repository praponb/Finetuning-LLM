from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("monthly_fee").desc())

ranked = (
    df_subscriptions
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
)

result = ranked.select("id", "plan", "status", "latest_rank")
