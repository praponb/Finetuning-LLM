from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name", "location").orderBy("budget", "id").rowsBetween(-2, -1)
w2 = Window.partitionBy("location")
w3 = Window.partitionBy("dept_name").orderBy(F.col("budget").desc(), "id")

result = (
    df_departments
    .withColumn("budget_avg_2_prior", F.avg("budget").over(w))
    .withColumn("budget_share", F.round((F.col("budget") / F.sum("budget").over(w2)), 4))
    .withColumn("event_seq", F.row_number().over(w3))
    .withColumn("location_min_budget", F.min("budget").over(w2))
)
