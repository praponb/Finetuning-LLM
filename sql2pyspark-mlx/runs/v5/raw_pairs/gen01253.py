from pyspark.sql import functions as F

base = (
    df_employees
    .filter(F.col("hire_date") > F.lit("2023-02-13").cast("date"))
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "salary",
        "hire_date",
        "level",
        F.when(F.col("is_remote"), "Yes").otherwise("No").alias("is_remote_label"),
        F.coalesce("level", F.lit("OTHER")).alias("resolved_level"),
    )
)

result = (
    base
    .groupBy("level")
    .agg(
        F.percentile_approx("salary", [0.25, 0.5, 0.75]).alias("salary_quartiles"),
        F.stddev_samp("salary").alias("salary_stddev"),
        F.sort_array(F.collect_set("level")).alias("level_list"),
        F.count(F.when(F.col("level") == "L4", 1)).alias("l4_rows"),
        F.stddev_samp("salary").alias("salary_stddev_2"),
    )
    .filter(F.col("salary_stddev") > 0)
)
