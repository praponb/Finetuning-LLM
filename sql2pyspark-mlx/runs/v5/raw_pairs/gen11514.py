from pyspark.sql import functions as F

enriched = (
    df_employees
    .alias("e")
    .join(df_employees.alias("b"), F.col("e.manager_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.hire_date").alias("employee_hire_date"))
)

result = (
    enriched
    .groupBy(
        F.when(F.col("salary") >= 134000, "LARGE").otherwise("SMALL").alias("salary_tier"),
    )
    .agg(
        F.avg(F.when(F.col("job_title") == "Analyst", F.col("salary"))).alias("analyst_avg_salary"),
        F.sum("salary").alias("total_salary"),
        F.round(F.avg("salary"), 2).alias("avg_salary"),
        F.round(F.sum("salary") / F.nullif(F.count(F.when(F.col("level") == "L3", 1)), F.lit(0)), 2).alias("salary_per_l3"),
        F.countDistinct("level").alias("distinct_levels"),
    )
)
