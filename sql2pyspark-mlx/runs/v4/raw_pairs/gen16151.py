from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("job_title").isin(["Engineer", "Designer"]))
    .groupBy(F.date_trunc("MONTH", "hire_date").alias("hire_month"), "is_remote")
    .agg(
        F.avg("salary").alias("avg_salary"),
        F.countDistinct("salary").alias("unique_levels"),
    )
    .filter(F.col("unique_levels") > 1)
    .orderBy(F.col("avg_salary").desc(), "hire_month")
)
