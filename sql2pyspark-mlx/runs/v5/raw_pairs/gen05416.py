from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("salary").between(76000, 147000))
    .groupBy(
        F.when(F.year("hire_date") < 2024, "before_2024").when(F.year("hire_date") == 2024, "2024").otherwise("after_2024").alias("hire_cohort"),
    )
    .agg(
        F.round(F.stddev("salary"), 2).alias("stddev_salary"),
        F.min("salary").alias("min_salary"),
    )
    .orderBy("hire_cohort")
)
