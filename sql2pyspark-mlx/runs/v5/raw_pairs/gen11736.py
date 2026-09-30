from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("start_date")

result = (
    df_subscriptions
    .filter(F.col("start_date").between("2023-07-01", "2024-03-01"))
    .select("id", "plan", "start_date", F.rank().over(w).alias("start_date_rank"))
)
