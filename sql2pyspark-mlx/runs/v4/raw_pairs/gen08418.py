from pyspark.sql import functions as F

with_dims = (
    df_employees
    .alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "inner")
    .select("e.*", F.col("d.dept_name"))
)

with_dims_employees = (
    with_dims
    .alias("src")
    .join(df_departments.alias("p"), F.col("src.dept_id") == F.col("p.id"), "left")
    .select("src.*", F.col("p.location"))
)

valid = (
    with_dims_employees
    .filter(F.col("hire_date") < "2025-07-22")
    .select(
        "*",
        F.trim("job_title").alias("clean_job_title"),
        F.date_format("hire_date", "yyyy-MM").alias("hire_date_label"),
        F.coalesce("job_title", F.lit("DIRECT")).alias("resolved_job_title"),
    )
)

result = (
    valid
    .groupBy("dept_name")
    .pivot("location", ["Tokyo", "Bangkok", "New York"])
    .agg(F.sum("salary").alias("value_sum"), F.count(F.lit(1)).alias("cnt"))
    .select(
        "dept_name",
        F.col("Tokyo_value_sum").alias("tokyo_value_sum"),
        F.col("Tokyo_cnt").alias("tokyo_cnt"),
        F.col("Bangkok_value_sum").alias("bangkok_value_sum"),
        F.col("Bangkok_cnt").alias("bangkok_cnt"),
        F.col("New York_value_sum").alias("new_york_value_sum"),
        F.col("New York_cnt").alias("new_york_cnt"),
    )
)
