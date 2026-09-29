from pyspark.sql import functions as F

joined = (
    df_employees
    .alias("e")
    .join(df_departments.alias("b"), F.col("e.dept_id") == F.col("b.id"), "inner")
    .select("e.*", F.col("b.dept_name"))
)

result = (
    joined
    .rollup(
        F.date_trunc("month", "hire_date").alias("activity_month"),
        F.when(F.col("salary") >= 148000, "PREMIUM").otherwise("STANDARD").alias("salary_tier"),
    )
    .agg(
        F.stddev_samp("salary").alias("salary_stddev"),
        F.sum(F.when(F.col("level") == "L4", F.col("salary")).otherwise(0)).alias("l4_salary"),
        F.count("*").alias("row_count"),
        F.countDistinct("job_title").alias("distinct_job_titles"),
        F.count_if(F.col("dept_name") == "Engineering").alias("engineering_events"),
        F.count(F.when(F.col("job_title") == "Manager", 1)).alias("manager_count"),
    )
    .orderBy(F.col("salary_tier").desc_nulls_last())
)
