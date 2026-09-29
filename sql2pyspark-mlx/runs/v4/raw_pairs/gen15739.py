from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("start_date").desc(), "id")

result = (
    df_subscriptions
    .filter(F.month("end_date").isin([1, 3, 10]))
    .withColumn("pos", F.row_number().over(w))
    .filter(F.col("pos") == 1)
    .select("id", "status", "start_date", "monthly_fee")
)
