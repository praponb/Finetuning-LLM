from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("start_date", "id")

result = (
    df_subscriptions
    .select("id", "plan", "start_date", F.ntile(4).over(w).alias("bucket"))
    .orderBy(F.col("bucket").desc())
)
