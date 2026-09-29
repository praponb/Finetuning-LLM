from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("started_at")

result = (
    df_sessions
    .withColumn("pos", F.dense_rank().over(w))
    .filter(F.col("pos") <= 3)
    .orderBy("browser")
)
