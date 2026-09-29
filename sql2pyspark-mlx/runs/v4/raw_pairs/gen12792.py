from pyspark.sql import functions as F

valid_employees = (
    df_employees
    .filter(F.col("email").like("%@gmail.com") | (F.col("dept_id") == 10))
    .withColumn("email_lower", F.lower("email"))
)

salary_bucket_summary = (
    valid_employees
    .groupBy(
        F.when(F.col("salary") >= 152000, "high").when(F.col("salary") >= 76000, "mid").otherwise("low").alias("salary_bucket"),
        "manager_id",
    )
    .agg(
        F.round(F.avg("salary"), 2).alias("avg_salary"),
        F.sum("salary").alias("total_salary"),
    )
)

result = salary_bucket_summary.orderBy(F.col("avg_salary").desc())
