from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy("budget", "id").rowsBetween(-3, Window.currentRow)
w_ordered = Window.orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("dept_name", "location")

scored = (
    df_departments
    .withColumn("budget_avg_3", F.avg("budget").over(w))
    .withColumn("running_budget", F.sum("budget").over(w_ordered))
    .withColumn("budget_share", F.round((F.col("budget") / F.sum("budget").over(w3)), 4))
)

result = (
    scored
    .groupBy("dept_name")
    .agg(
        F.count(F.when(F.col("location") == "Tokyo", 1)).alias("tokyo_count"),
        F.count(F.when(F.col("location") == "Tokyo", 1)).alias("tokyo_rows"),
        F.count_if(F.col("dept_name") == "HR").alias("hr_events"),
    )
)
