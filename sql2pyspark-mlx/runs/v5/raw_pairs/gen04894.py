from pyspark.sql import functions as F

employees_by_department = (
    df_employees
    .filter(F.col("hire_date") <= "2024-11-14")
    .groupBy("dept_id")
    .agg(
        F.sort_array(F.collect_set("level")).alias("level_list"),
        F.count("*").alias("row_count"),
    )
)

result = (
    df_departments.alias("d")
    .join(employees_by_department.alias("t"), F.col("d.id") == F.col("t.dept_id"), "left")
    .select(
        F.col("d.dept_name"),
        F.col("d.location"),
        F.col("t.level_list"),
        F.col("t.row_count"),
    )
)
