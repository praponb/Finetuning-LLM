from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("is_remote") & F.col("job_title").isin(["Intern", "Designer", "Director"]))
    .groupBy("is_remote")
    .agg(
        F.countDistinct("salary").alias("unique_job_titles"),
        F.min("hire_date").alias("first_hire_date"),
    )
)
