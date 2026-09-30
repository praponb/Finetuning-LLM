from pyspark.sql import functions as F

filtered_employees = (
    df_employees
    .filter(~((F.col("dept_id").isin([2, 35, 36]) & (F.col("dept_id") == 34)) | F.lower("email").like("%iam%")))
)

job_title_summary = (
    filtered_employees
    .groupBy(
        "job_title",
        F.when(F.col("salary") >= 120000, "high").when(F.col("salary") >= 60000, "mid").otherwise("low").alias("salary_bucket"),
    )
    .agg(F.countDistinct("salary").alias("unique_depts"))
)

result = job_title_summary.orderBy("unique_depts")
