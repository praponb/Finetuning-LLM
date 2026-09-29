from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("job_title").isNull())
    .groupBy(F.date_trunc("MONTH", "hire_date").alias("hire_month"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("job_title") == "Designer", 1).otherwise(0))) / F.count("*")), 1).alias("pct_designer"),
    )
    .orderBy("hire_month", "pct_designer")
)
