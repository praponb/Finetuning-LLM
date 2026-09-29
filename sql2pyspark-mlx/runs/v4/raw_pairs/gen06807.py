from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("budget", "id").rowsBetween(-2, -1)
w2 = Window.partitionBy("location").orderBy(F.col("budget").desc_nulls_first())
w3 = Window.partitionBy("location")
w4 = Window.partitionBy("location").orderBy("budget", "id")

result = (
    df_departments
    .withColumn("budget_avg_2_prior", F.avg("budget").over(w))
    .withColumn("budget_percent_rank", F.percent_rank().over(w2))
    .withColumn("budget_share", F.round((F.col("budget") / F.sum("budget").over(w3)), 4))
    .withColumn("budget_bucket", F.ntile(3).over(w4))
    .withColumn("event_seq", F.row_number().over(w4))
)
