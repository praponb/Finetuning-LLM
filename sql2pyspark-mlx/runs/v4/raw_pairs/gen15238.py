from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy(F.col("started_at").desc(), "id")

result = (
    df_sessions
    .withColumn("rnk", F.row_number().over(w))
    .filter(F.col("rnk") == 1)
    .select("id", "device", "started_at", "customer_id")
    .orderBy(F.col("started_at").desc())
)
