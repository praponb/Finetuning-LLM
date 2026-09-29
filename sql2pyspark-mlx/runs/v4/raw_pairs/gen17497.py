from pyspark.sql import functions as F

result = (
    df_employees
    .withColumn("salary_tier", F.when(F.col("salary") >= 122000, "HIGH").otherwise("LOW"))
    .withColumn("activity_month", F.date_trunc("MONTH", "hire_date"))
    .rollup("salary_tier", "activity_month")
    .agg(
        F.count(F.when(F.col("job_title") == "Engineer", 1)).alias("engineer_count"),
        F.stddev_samp("salary").alias("salary_stddev"),
        F.count(F.when(F.col("job_title") == "Designer", 1)).alias("designer_rows"),
        F.percentile_approx("salary", [0.25, 0.5, 0.75]).alias("salary_quartiles"),
        F.grouping("salary_tier").alias("grouping_id"),
    )
    .orderBy(F.col("engineer_count").desc_nulls_first())
)
