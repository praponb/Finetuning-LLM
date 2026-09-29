from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location", "dept_name").orderBy("location_length", "id")
w2 = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_departments
    .filter(F.col("location").isNotNull())
    .withColumn("budget_rounded", F.round("budget", 1))
    .withColumn("location_length", F.length("location"))
)

eligible = (
    base
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.col("c.is_remote")), "left_anti")
)

kept = (
    eligible
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.col("x.job_title") == "Intern"), "left_anti")
)

result = (
    kept
    .withColumn("prev_location_length", F.lag("location_length", 1).over(w))
    .withColumn("running_location_length", F.sum("location_length").over(w2))
)
