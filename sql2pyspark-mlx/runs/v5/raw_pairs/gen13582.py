from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(
        F.when(F.col("salary") >= 156000, "HIGH").otherwise("LOW").alias("salary_tier"),
        F.date_trunc("QUARTER", "hire_date").alias("quarter_start"),
    )
    .agg(
        F.round(F.sum("salary") / F.nullif(F.count(F.when(F.col("job_title") == "Manager", 1)), F.lit(0)), 2).alias("salary_per_manager"),
        F.sum("salary").alias("total_salary"),
        F.percentile_approx("salary", 0.9).alias("salary_p90"),
        F.stddev_samp("salary").alias("salary_stddev"),
        F.round(F.sum("salary") / F.nullif(F.count(F.when(F.col("level") == "L1", 1)), F.lit(0)), 2).alias("salary_per_l1"),
    )
    .filter(F.col("salary_per_l1") < 1000000)
    .orderBy(F.col("quarter_start").desc_nulls_last())
)
