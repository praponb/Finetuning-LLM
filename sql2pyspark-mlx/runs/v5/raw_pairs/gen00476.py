from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("started_at").desc())

result = (
    df_sessions
    .filter(F.to_date("started_at") < "2023-08-01")
    .select("id", "customer_id", "started_at", F.rank().over(w).alias("rnk"))
    .orderBy("customer_id", "started_at")
)
