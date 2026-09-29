from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("monthly_fee", "id")

result = (
    df_subscriptions
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "plan", "start_date", "end_date", "monthly_fee", "status")
)
