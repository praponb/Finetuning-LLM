from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(
        F.when(F.col("salary") >= 122000, "high").when(F.col("salary") >= 41000, "mid").otherwise("low").alias("salary_bucket"),
        F.date_format("hire_date", "yyyy-MM").alias("hire_month"),
        "is_remote",
    )
    .agg(F.avg("salary").alias("avg_salary"))
    .filter(F.col("avg_salary") < 110000)
)
