from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("end_date").desc())

result = (
    df_subscriptions
    .select("id", "plan", "end_date", F.dense_rank().over(w).alias("rn"))
    .filter(F.col("rn") <= 3)
    .select("id", "plan", "end_date")
    .orderBy(F.col("id").desc())
)
