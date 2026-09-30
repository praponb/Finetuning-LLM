from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())

with_last = (
    df_departments
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.job_title").alias("employee_job_title"))
)

result = (
    with_last
    .groupBy("location")
    .agg(
        F.percentile_approx("budget", [0.25, 0.5, 0.75]).alias("budget_quartiles"),
        F.sum("budget").alias("total_budget"),
        F.round(F.avg("budget"), 2).alias("avg_budget"),
        F.count(F.when(F.col("location") == "Bangkok", 1)).alias("bangkok_rows"),
        F.stddev_samp("budget").alias("budget_stddev"),
    )
    .orderBy(F.col("total_budget").desc())
)
