from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("monthly_fee")

result = (
    df_subscriptions
    .select("id", "customer_id", "monthly_fee", F.rank().over(w).alias("rn"))
    .filter(F.col("rn") <= 3)
    .select("id", "customer_id", "monthly_fee")
)
