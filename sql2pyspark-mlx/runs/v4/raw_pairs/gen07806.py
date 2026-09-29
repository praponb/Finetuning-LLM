from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("job_title", F.year("hire_date").alias("hire_year"))
    .agg(
        F.min("salary").alias("min_salary"),
        F.concat_ws(", ", F.sort_array(F.collect_set("job_title"))).alias("job_title_values"),
    )
    .orderBy(F.col("min_salary").desc(), "job_title_values")
)
