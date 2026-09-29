from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.lower("full_name").like("%vy %"))
    .groupBy(
        "level",
        F.when(F.col("salary") >= 139000, "high").when(F.col("salary") >= 48000, "mid").otherwise("low").alias("salary_bucket"),
    )
    .agg(
        F.max("hire_date").alias("last_hire_date"),
        F.min("salary").alias("min_salary"),
    )
    .orderBy(F.col("min_salary").desc(), F.col("last_hire_date").desc())
)
