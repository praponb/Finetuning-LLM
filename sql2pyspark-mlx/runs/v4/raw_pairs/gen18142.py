from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy(F.col("salary").desc())

latest = (
    df_employees
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

result = (
    latest
    .rollup(
        F.when(F.col("salary") >= 155000, "LARGE").otherwise("SMALL").alias("salary_tier"),
        F.date_trunc("MONTH", "hire_date").alias("activity_month"),
    )
    .agg(
        F.sum(F.when(F.col("job_title") == "Engineer", F.col("salary")).otherwise(0)).alias("engineer_salary"),
        F.round(F.avg("salary"), 2).alias("avg_salary"),
        F.percentile_approx("salary", 0.95).alias("salary_p95"),
        F.round(F.avg("salary"), 2).alias("avg_salary_2"),
    )
    .orderBy(F.col("avg_salary_2").asc_nulls_first())
)
