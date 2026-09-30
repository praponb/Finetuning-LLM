from pyspark.sql import functions as F

result = (
    df_employees
    .withColumn("quarter_start", F.date_trunc("QUARTER", "hire_date"))
    .withColumn("salary_tier", F.when(F.col("salary") >= 148000, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["quarter_start", "salary_tier"], ["quarter_start"]],
        "quarter_start",
        "salary_tier",
    )
    .agg(
        F.round(F.sum("salary") / F.nullif(F.count(F.when(F.col("job_title") == "Manager", 1)), F.lit(0)), 2).alias("salary_per_manager"),
        F.count(F.when(F.col("job_title") == "Manager", 1)).alias("manager_count"),
        F.percentile_approx("salary", 0.95).alias("salary_p95"),
        F.count_if(F.col("job_title") == "Designer").alias("designer_events"),
        F.sum(F.when(F.col("job_title") == "Analyst", F.col("salary")).otherwise(0)).alias("analyst_salary"),
    )
    .orderBy("quarter_start")
)
