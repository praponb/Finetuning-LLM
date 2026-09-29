from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(F.quarter("hire_date").alias("hire_quarter"))
    .agg(
        F.sort_array(F.collect_set("level")).alias("level_list"),
        F.sum(F.when(F.col("job_title") == "Manager", F.col("salary")).otherwise(0)).alias("manager_salary"),
        F.count("*").alias("employee_count"),
    )
    .orderBy(F.col("manager_salary").desc())
)
