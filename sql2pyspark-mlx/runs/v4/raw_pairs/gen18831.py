from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("end_date").desc())

result = (
    df_subscriptions
    .withColumn("pos", F.rank().over(w))
    .filter(F.col("pos") <= 3)
)
