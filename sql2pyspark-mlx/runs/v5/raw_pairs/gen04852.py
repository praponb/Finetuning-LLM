from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("hire_date").between("2023-07-01", "2025-04-12") & F.col("manager_id").isNull() & F.col("is_remote"))
    .groupBy(
        F.when(F.year("hire_date") < 2024, "before_2024").when(F.year("hire_date") == 2024, "2024").otherwise("after_2024").alias("hire_cohort"),
        "job_title",
    )
    .agg(
        F.countDistinct("salary").alias("unique_levels"),
        F.min("salary").alias("min_salary"),
    )
    .filter(F.col("min_salary") < 82000)
    .orderBy(F.col("unique_levels").desc())
)
