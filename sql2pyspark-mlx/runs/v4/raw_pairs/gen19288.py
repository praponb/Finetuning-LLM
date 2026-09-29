from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("started_at").desc(), "id")

result = (
    df_sessions
    .filter(F.col("started_at") >= "2025-04-01")
    .withColumn("pos", F.row_number().over(w))
    .filter(F.col("pos") == 1)
    .select("id", "country", "started_at", "customer_id")
)
