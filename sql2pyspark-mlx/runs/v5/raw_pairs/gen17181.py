from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy(F.col("started_at").desc())

result = (
    df_sessions
    .filter(F.col("customer_id").isin([1, 20, 32, 36]))
    .select("id", "device", "started_at", F.rank().over(w).alias("rnk"))
)
