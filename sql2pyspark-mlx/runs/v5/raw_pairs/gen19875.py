from pyspark.sql import functions as F

result = (
    df_employees
    .rollup(F.date_trunc("WEEK", "hire_date").alias("week_start"), "level")
    .agg(
        F.stddev_samp("salary").alias("salary_stddev"),
        F.size(F.collect_set("level")).alias("num_levels"),
        F.sum(F.when(F.col("level") == "L2", F.col("salary")).otherwise(0)).alias("l2_salary"),
    )
)
