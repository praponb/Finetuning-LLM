from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("start_date").desc())

result = (
    df_subscriptions
    .withColumn("rnk", F.rank().over(w))
    .filter(F.col("rnk") <= 3)
)
