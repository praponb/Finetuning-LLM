from pyspark.sql import functions as F

result = (
    df_employees
    .rollup(F.date_trunc("year", "hire_date").alias("year_start"))
    .agg(
        F.max(F.when(F.col("job_title") == "Manager", F.col("salary"))).alias("manager_max_salary"),
        F.countDistinct("level").alias("distinct_levels"),
        F.sum(F.when(F.col("level") == "L4", F.col("salary")).otherwise(0)).alias("l4_salary"),
        F.round(F.sum("salary") / F.nullif(F.count(F.when(F.col("job_title") == "Engineer", 1)), F.lit(0)), 2).alias("salary_per_engineer"),
        F.round(F.avg("salary"), 2).alias("avg_salary"),
        F.round(F.avg("salary"), 2).alias("avg_salary_2"),
    )
    .filter(F.col("avg_salary_2") > 0)
)
