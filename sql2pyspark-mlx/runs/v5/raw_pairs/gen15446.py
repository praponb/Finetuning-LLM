from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("started_at").desc())

result = (
    df_sessions
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") <= 2)
)
