from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.orderBy("budget", "id")
w2 = Window.partitionBy("location")

result = (
    df_departments
    .withColumn("budget_bucket", F.ntile(5).over(w_recent))
    .withColumn("location_avg_budget", F.avg("budget").over(w2))
)
