from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "left")
    .filter(F.col("emp.level") == "L5")
    .select(
        F.col("emp.is_remote").alias("employee_is_remote"),
        F.col("mgr.manager_id").alias("manager_manager_id"),
        F.col("mgr.email").alias("manager_email"),
        F.col("mgr.level").alias("manager_level"),
        F.trim(F.col("emp.level")).alias("clean_level"),
    )
    .orderBy("manager_level", F.col("clean_level").desc())
)
