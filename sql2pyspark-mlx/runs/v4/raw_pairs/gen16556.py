from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("job_title").isNotNull())
    .groupBy(
        F.quarter("hire_date").alias("hire_quarter"),
        F.when(F.col("salary") >= 146000, "high").when(F.col("salary") >= 54000, "mid").otherwise("low").alias("salary_bucket"),
    )
    .agg(
        F.round(F.stddev("salary"), 2).alias("stddev_salary"),
        F.count(F.when(F.col("job_title") == "Analyst", 1)).alias("analyst_count"),
    )
    .filter(F.col("stddev_salary") > 29)
    .select("hire_quarter", "salary_bucket", "stddev_salary", "analyst_count")
    .orderBy("salary_bucket")
)
