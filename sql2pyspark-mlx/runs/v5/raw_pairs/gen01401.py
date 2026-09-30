from pyspark.sql import functions as F

cleaned = (
    df_employees
    .filter(F.col("hire_date") > F.lit("2023-06-14").cast("date"))
    .select(
        "id",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "is_remote",
        "level",
        F.length("full_name").alias("full_name_length"),
    )
)

result = (
    cleaned
    .groupBy("dept_id")
    .pivot("level", ["L5", "L2"])
    .agg(F.sum("salary").alias("amount"))
)
