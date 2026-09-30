from pyspark.sql import functions as F

valid_employees = df_employees.filter(F.year("hire_date") == 2024)

employees_stats = (
    valid_employees
    .groupBy("is_remote")
    .agg(
        F.max("salary").alias("max_salary"),
        F.sort_array(F.collect_set("level")).alias("level_list"),
    )
)

result = (
    employees_stats
    .filter(F.col("max_salary") >= 95000)
    .orderBy(F.col("max_salary").desc(), "is_remote")
    .limit(5)
)
