from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(
        F.when(F.col("salary") >= 127000, "HIGH").otherwise("LOW").alias("salary_tier"),
        F.date_trunc("YEAR", "hire_date").alias("year_start"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.avg("salary"), 2).alias("avg_salary"),
    )
    .orderBy(F.col("salary_tier").asc_nulls_first())
)
