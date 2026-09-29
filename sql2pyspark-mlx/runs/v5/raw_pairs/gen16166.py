from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("location").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_departments
    .filter(F.col("dept_name") == "HR")
    .withColumn("clean_dept_name", F.trim("dept_name"))
)

result = (
    cleaned
    .withColumn("running_avg_budget", F.avg("budget").over(w_ordered))
    .withColumn("running_sum_budget", F.sum("budget").over(w_ordered))
    .withColumn("running_max_budget", F.max("budget").over(w_ordered))
    .withColumn("running_min_budget", F.min("budget").over(w_ordered))
)
