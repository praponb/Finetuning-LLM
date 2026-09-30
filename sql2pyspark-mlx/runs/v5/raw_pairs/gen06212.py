from pyspark.sql import functions as F

base = (
    df_employees
    .filter(F.col("level").isNotNull() & F.col("full_name").isNotNull())
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "salary",
        "level",
        F.col("is_remote").cast("int").alias("is_remote_flag"),
    )
)

result = (
    base
    .select("id", "level")
    .intersectAll(
        df_employees
        .filter(~F.col("job_title").isin(["Director", "Intern", "Manager"]))
        .select("id", "level")
    )
)
