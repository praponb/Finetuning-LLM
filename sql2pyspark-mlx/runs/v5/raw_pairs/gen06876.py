from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())

cleaned = (
    df_departments
    .filter(F.col("dept_name").isNotNull() & (F.col("dept_name").eqNullSafe(F.lit("HR"))))
    .withColumn("budget_rounded", F.round("budget", 1))
    .withColumn("location_clean", F.coalesce("location", F.lit("N/A")))
)

result = (
    cleaned
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.level").alias("employee_level"), F.col("c.hire_date"))
)
