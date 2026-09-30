from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(
        F.when(F.col("salary") >= 130000, "LARGE").otherwise("SMALL").alias("salary_tier"),
    )
    .agg(
        F.max(F.when(F.col("level") == "L4", F.col("salary"))).alias("l4_max_salary"),
        F.sort_array(F.collect_set("job_title")).alias("job_title_list"),
        F.min("salary").alias("min_salary"),
        F.stddev_samp("salary").alias("salary_stddev"),
    )
)
