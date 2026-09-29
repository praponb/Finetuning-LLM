from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser", "device").orderBy(F.col("started_at").desc())

result = (
    df_sessions
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
)
