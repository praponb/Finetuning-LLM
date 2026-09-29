from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(
        F.when(F.year("hire_date") < 2024, "before_2024").when(F.year("hire_date") == 2024, "2024").otherwise("after_2024").alias("hire_cohort"),
        "job_title",
    )
    .agg(
        F.min("salary").alias("min_salary"),
        F.round(F.avg("salary"), 2).alias("avg_salary"),
    )
    .filter(F.col("avg_salary") >= 54)
)
