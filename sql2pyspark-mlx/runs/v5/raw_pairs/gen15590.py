from pyspark.sql import functions as F

valid = (
    df_employees
    .filter(F.upper(F.trim("full_name")) == "OLIVIA GARCIA")
    .select("*", F.col("is_remote").cast("int").alias("is_remote_flag"))
)

result = (
    valid
    .groupBy(F.date_trunc("MONTH", "hire_date").alias("activity_month"))
    .agg(
        F.sum("salary").alias("total_salary"),
        F.count(F.when(F.col("level") == "L3", 1)).alias("l3_rows"),
        F.stddev_samp("salary").alias("salary_stddev"),
    )
)
