from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy("started_at", "id")

result = (
    df_sessions
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") <= 3)
    .select("id", "browser", "started_at", "device")
    .orderBy("device")
)
