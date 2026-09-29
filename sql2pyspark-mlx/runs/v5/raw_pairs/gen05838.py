from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("hire_date") <= "2024-01-01")
    .agg(
        F.sort_array(F.collect_set("job_title")).alias("job_title_list"),
        F.round(F.avg("salary"), 2).alias("avg_salary"),
    )
    .orderBy(F.col("avg_salary").desc())
)
