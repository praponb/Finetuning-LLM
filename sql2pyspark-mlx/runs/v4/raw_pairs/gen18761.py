from pyspark.sql import functions as F

result = (
    df_employees
    .filter(~F.col("is_remote") & F.col("dept_id").isin([16, 22, 25]))
    .groupBy("level")
    .agg(
        (F.max("salary") - F.min("salary")).alias("salary_range"),
        F.avg("salary").alias("avg_salary"),
        F.count(F.when(F.col("job_title") == "Director", 1)).alias("director_count"),
    )
    .filter(F.col("director_count") > 1)
    .drop("director_count")
    .orderBy(F.col("avg_salary").desc())
)
