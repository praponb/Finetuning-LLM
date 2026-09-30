from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())

valid = (
    df_departments
    .filter(F.col("budget") < 1861000)
    .withColumn("budget_tier", F.when(F.col("budget") >= 3061000, "High").when(F.col("budget") >= 1207000, "Medium").otherwise("Low"))
    .withColumn("dept_name_filled", F.coalesce("dept_name", F.lit("UNKNOWN")))
)

result = (
    valid
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.budget", F.col("c.salary").alias("employee_salary"))
)
