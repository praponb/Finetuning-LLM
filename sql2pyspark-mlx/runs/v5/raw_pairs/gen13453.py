from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter((F.col("m.hire_date") < "2023-03-01") & F.col("e.full_name").isNotNull())
    .select(
        F.col("e.full_name").alias("employee_full_name"),
        F.col("e.level").alias("employee_level"),
        F.col("e.manager_id").alias("employee_manager_id"),
        F.col("m.is_remote").alias("manager_is_remote"),
        F.col("m.dept_id").alias("manager_dept_id"),
        F.col("m.level").alias("manager_level"),
        F.concat_ws("-", F.col("m.level"), F.col("m.full_name")).alias("level_full_name_key"),
    )
)
