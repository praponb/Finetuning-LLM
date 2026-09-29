from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("hire_date").between("2024-07-14", "2025-08-31"))
    .groupBy(
        F.when(F.col("salary") >= 148000, "high").when(F.col("salary") >= 44000, "mid").otherwise("low").alias("salary_bucket"),
    )
    .agg(
        F.min("hire_date").alias("min_hire_date"),
        F.max("hire_date").alias("latest_hire_date"),
    )
    .orderBy(F.col("latest_hire_date").desc(), "salary_bucket")
    .limit(1)
)
