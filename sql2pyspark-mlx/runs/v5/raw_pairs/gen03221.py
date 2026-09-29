from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("started_at").desc())

result = (
    df_sessions
    .withColumn("rnk", F.dense_rank().over(w))
    .filter(F.col("rnk") <= 3)
)
