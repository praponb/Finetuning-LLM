from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("end_date", "id")

result = (
    df_subscriptions
    .filter(~F.col("status").isin(["CANCELLED", "PAUSED"]))
    .select("id", "customer_id", "end_date", F.ntile(2).over(w).alias("end_date_ntile"))
)
