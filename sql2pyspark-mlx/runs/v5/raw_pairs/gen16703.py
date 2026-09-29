from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("manager_id").isin([6, 31, 34]) & F.col("dept_id").isin([7, 23, 29, 37])) | F.month("hire_date").isin([5, 6, 9]))
    .groupBy(F.date_format("hire_date", "yyyy-MM").alias("hire_month"), "job_title")
    .agg(
        F.round(F.stddev("salary"), 2).alias("stddev_salary"),
        F.sort_array(F.collect_set("level")).alias("level_list"),
        F.round(((100 * F.sum(F.when(F.col("level") == "L3", 1).otherwise(0))) / F.count("*")), 1).alias("pct_l3"),
    )
    .orderBy("pct_l3", F.col("stddev_salary").desc())
)
