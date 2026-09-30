from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("monthly_fee")

result = (
    df_subscriptions
    .select("id", "plan", "monthly_fee", "status", F.dense_rank().over(w).alias("rn"))
    .filter(F.col("rn") <= 5)
)
