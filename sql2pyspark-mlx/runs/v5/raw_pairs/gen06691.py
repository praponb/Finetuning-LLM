from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.length("email") < 7)
    .groupBy(
        F.when(F.year("hire_date") < 2024, "before_2024").when(F.year("hire_date") == 2024, "2024").otherwise("after_2024").alias("hire_cohort"),
    )
    .agg(
        F.sum("salary").alias("total_salary"),
        F.avg("salary").alias("avg_salary"),
    )
    .filter(F.col("avg_salary") >= 143000)
    .drop("avg_salary")
    .orderBy(F.col("hire_cohort").desc())
)
