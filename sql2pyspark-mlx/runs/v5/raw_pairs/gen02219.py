from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("started_at")

result = (
    df_sessions
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") <= 2)
    .orderBy(F.col("customer_id").desc())
)
