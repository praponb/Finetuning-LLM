from pyspark.sql import functions as F

employees_by_department = (
    df_employees
    .filter(F.col("job_title") == "Manager")
    .groupBy("dept_id")
    .agg(F.count(F.when(F.col("level") == "L2", 1)).alias("l2_count"))
)

result = (
    df_departments.alias("d")
    .join(employees_by_department.alias("ebd"), F.col("d.id") == F.col("ebd.dept_id"), "left")
    .select(F.col("d.dept_name"), F.col("d.budget"), F.col("ebd.l2_count"))
    .orderBy("l2_count")
)
