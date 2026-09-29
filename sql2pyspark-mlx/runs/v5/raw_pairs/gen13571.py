from pyspark.sql import functions as F

base = (
    df_employees
    .filter((F.col("hire_date") > F.lit("2023-06-30").cast("date")) & (F.col("dept_id") == 33))
    .select(
        "id",
        "full_name",
        "email",
        "manager_id",
        "job_title",
        "salary",
        "is_remote",
        F.round((F.col("salary") * 1.15), 2).alias("salary_scaled"),
        F.when(F.col("is_remote"), "Yes").otherwise("No").alias("is_remote_label"),
        F.coalesce("job_title", F.lit("DIRECT")).alias("resolved_job_title"),
    )
)

result = (
    base
    .groupBy("manager_id")
    .pivot("job_title", ["Engineer", "Director"])
    .agg(F.max("salary_scaled"))
)
