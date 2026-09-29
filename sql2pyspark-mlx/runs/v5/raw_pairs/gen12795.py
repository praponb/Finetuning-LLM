from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())

valid = (
    df_departments
    .filter(F.col("dept_name").isNotNull() & ~F.col("dept_name").isin(["Finance", "Sales"]))
    .withColumn("dept_name_lower", F.lower("dept_name"))
    .withColumn("clean_dept_name", F.trim("dept_name"))
    .withColumn("resolved_location", F.coalesce("location", F.lit("NONE")))
)

result = (
    valid
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.hire_date").alias("employee_hire_date"))
)
