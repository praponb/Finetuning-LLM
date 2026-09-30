from pyspark.sql import functions as F

result = (
    df_employees
    .rollup("dept_id", "level")
    .agg(
        F.sum(F.when(F.col("job_title") == "Designer", F.col("salary"))).alias("designer_total_salary"),
        F.stddev_samp("salary").alias("salary_stddev"),
        F.count_if(F.col("job_title") == "Designer").alias("designer_events"),
        F.count("*").alias("num_rows"),
        F.count(F.when(F.col("level") == "L3", 1)).alias("l3_count"),
        F.round(F.sum("salary") / F.nullif(F.count(F.when(F.col("level") == "L3", 1)), F.lit(0)), 2).alias("salary_per_l3"),
    )
    .orderBy(F.col("l3_count").desc_nulls_last())
)
