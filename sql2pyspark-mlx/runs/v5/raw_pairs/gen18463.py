from pyspark.sql import functions as F

base = (
    df_employees
    .filter(F.col("hire_date") > F.lit("2023-07-11").cast("date"))
    .select(
        "id",
        "full_name",
        "email",
        "manager_id",
        "job_title",
        "is_remote",
        "level",
        F.datediff(F.current_date(), "hire_date").alias("days_since_hire_date"),
        F.coalesce("job_title", F.lit("NONE")).alias("job_title_clean"),
    )
)

result = (
    base
    .select("manager_id")
    .exceptAll(
        df_employees
        .filter(F.col("level") == "L5")
        .select("manager_id")
    )
)
