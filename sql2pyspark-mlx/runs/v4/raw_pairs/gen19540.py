from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.upper("full_name").like("%ENR%"))
    .groupBy(
        F.when(F.col("salary") >= 154000, "high").when(F.col("salary") >= 45000, "mid").otherwise("low").alias("salary_bucket"),
        F.date_trunc("MONTH", "hire_date").alias("hire_month"),
    )
    .agg(
        F.max("salary").alias("max_salary"),
        F.round(((100 * F.sum(F.when(F.col("job_title") == "Director", 1).otherwise(0))) / F.count("*")), 1).alias("pct_director"),
    )
    .filter(F.col("max_salary") > 117000)
    .orderBy("hire_month")
)
