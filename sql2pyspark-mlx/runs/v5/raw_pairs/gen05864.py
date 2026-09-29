from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(F.date_trunc("MONTH", "hire_date").alias("hire_month"))
    .agg(
        F.sum("salary").alias("total_salary"),
        F.sort_array(F.collect_set("level")).alias("level_list"),
        F.sort_array(F.collect_set("job_title")).alias("job_title_list"),
    )
)
