from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy(F.col("started_at").desc())

result = (
    df_sessions
    .withColumn("row_num", F.dense_rank().over(w))
    .filter(F.col("row_num") <= 5)
    .select("id", "device", "started_at")
)
