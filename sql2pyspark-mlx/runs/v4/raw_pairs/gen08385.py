from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(
        F.when(F.col("salary") >= 157000, "high").when(F.col("salary") >= 57000, "mid").otherwise("low").alias("salary_bucket"),
    )
    .agg(
        F.count("*").alias("num_employees"),
        F.min("salary").alias("min_salary"),
        F.count(F.when(F.col("job_title") == "Intern", 1)).alias("intern_count"),
    )
    .filter(F.col("num_employees") < 5)
    .orderBy(F.col("min_salary").desc())
)
