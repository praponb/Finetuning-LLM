from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("start_date")

result = (
    df_subscriptions
    .select("id", "start_date", F.rank().over(w).alias("start_date_rank"))
)
