from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("start_date", "id")

result = (
    df_subscriptions
    .select("id", "customer_id", "start_date", F.ntile(2).over(w).alias("bucket"))
)
