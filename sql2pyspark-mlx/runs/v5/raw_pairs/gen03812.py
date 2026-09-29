from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("started_at")

result = (
    df_sessions
    .select("id", "device", "started_at", "customer_id", F.rank().over(w).alias("pos"))
    .filter(F.col("pos") <= 3)
    .select("id", "device", "started_at", "customer_id")
)
