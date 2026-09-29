from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())

latest_match = (
    df_departments
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.job_title"),
        F.col("c.hire_date").alias("employee_hire_date"),
    )
)

result = (
    latest_match
    .groupBy("dept_name")
    .agg(
        F.count(F.when(F.col("dept_name") == "Legal", 1)).alias("legal_rows"),
        F.sum(F.when(F.col("job_title") == "Designer", F.col("budget")).otherwise(0)).alias("designer_budget"),
        F.sum(F.when(F.col("job_title") == "Manager", F.col("budget")).otherwise(0)).alias("manager_budget"),
    )
    .orderBy("designer_budget")
)
