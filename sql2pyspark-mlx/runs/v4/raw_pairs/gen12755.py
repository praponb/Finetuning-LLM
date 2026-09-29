from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("budget", "id")
w2 = Window.partitionBy("location", "dept_name").orderBy(F.col("budget").asc_nulls_last())

result = (
    df_departments
    .withColumn("budget_bucket", F.ntile(5).over(w))
    .withColumn("budget_percent_rank", F.percent_rank().over(w2))
)
