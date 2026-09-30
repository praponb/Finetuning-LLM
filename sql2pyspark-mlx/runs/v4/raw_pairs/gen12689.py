from pyspark.sql import functions as F

employees_by_department = (
    df_employees
    .groupBy("dept_id")
    .agg(
        F.count("*").alias("row_count"),
        F.sum(F.when(F.col("level") == "L1", F.col("salary")).otherwise(0)).alias("l1_salary"),
    )
)

result = (
    df_departments.alias("d")
    .join(employees_by_department.alias("t"), F.col("d.id") == F.col("t.dept_id"), "left")
    .select(F.col("d.dept_name"), F.col("t.row_count"), F.col("t.l1_salary"))
    .orderBy(F.col("l1_salary").desc(), F.col("row_count").desc())
)
