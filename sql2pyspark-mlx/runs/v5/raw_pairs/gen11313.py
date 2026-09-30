from pyspark.sql import functions as F

base = df_employees.filter((F.year("hire_date") >= 2025) & (F.col("dept_id") == 5))

summary = (
    base
    .groupBy(
        "is_remote",
        F.when(F.col("salary") >= 164000, "high").when(F.col("salary") >= 73000, "mid").otherwise("low").alias("salary_bucket"),
    )
    .agg(F.round(F.avg("salary"), 2).alias("avg_salary"))
)

result = (
    summary
    .select("is_remote", "salary_bucket")
    .orderBy("salary_bucket")
)
