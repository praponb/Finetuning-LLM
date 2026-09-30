from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("dept_id") == 22)
    .groupBy("job_title")
    .agg(
        F.sum("salary").alias("total_salary"),
        F.sort_array(F.collect_set("job_title")).alias("job_title_list"),
    )
    .filter(F.col("total_salary") < 726000)
)
