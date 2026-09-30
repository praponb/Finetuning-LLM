from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("level").isin(["L2", "L5", "L1"]) & (F.col("dept_id").isin([5, 16, 19]) | F.month("hire_date").isin([2, 5, 7])))
    .agg(
        F.count("*").alias("employee_count"),
        F.sort_array(F.collect_set("job_title")).alias("job_title_list"),
        F.round(F.avg("salary"), 2).alias("avg_salary"),
    )
)
