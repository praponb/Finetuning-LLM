from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location")
w2 = Window.partitionBy("location", "dept_name").orderBy("budget", "id").rowsBetween(-6, -1)
w_running = Window.partitionBy("location").orderBy(F.col("budget").desc(), "id")

with_windows = (
    df_departments
    .withColumn("location_min_budget", F.min("budget").over(w))
    .withColumn("budget_avg_6_prior", F.avg("budget").over(w2))
    .withColumn("budget_bucket", F.ntile(3).over(w_running))
)

result = (
    with_windows
    .select("id", "location", "budget_avg_6_prior", "budget_bucket")
    .orderBy(F.col("budget_bucket").asc_nulls_first())
)
