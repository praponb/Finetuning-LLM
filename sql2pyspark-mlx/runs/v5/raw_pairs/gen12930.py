from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy(F.col("started_at").desc())

result = (
    df_sessions
    .filter(F.col("customer_id").isNull())
    .select("id", "device", "started_at", F.rank().over(w).alias("rnk"))
    .filter(F.col("rnk") == 1)
)
