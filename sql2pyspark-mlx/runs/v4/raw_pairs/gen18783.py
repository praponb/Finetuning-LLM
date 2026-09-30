from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(F.date_trunc("MONTH", "hire_date").alias("activity_month"))
    .agg(
        F.count(F.when(F.col("job_title") == "Manager", 1)).alias("manager_rows"),
        F.round(F.sum("salary") / F.nullif(F.count(F.when(F.col("job_title") == "Engineer", 1)), F.lit(0)), 2).alias("salary_per_engineer"),
        F.percentile_approx("salary", [0.25, 0.5, 0.75]).alias("salary_quartiles"),
        F.size(F.collect_set("level")).alias("num_levels"),
        F.max("salary").alias("max_salary"),
    )
)
