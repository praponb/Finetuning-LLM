from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title", "manager_id").orderBy(F.col("salary").desc(), "id")
w2 = Window.partitionBy("manager_id")

cleaned = (
    df_employees
    .filter(F.col("job_title").isNotNull())
    .select(
        "*",
        F.datediff(F.current_date(), "hire_date").alias("days_since_hire_date"),
    )
)

valid = (
    cleaned
    .filter(F.col("hire_date") >= F.to_date(F.lit("2023-03-07")))
    .select(
        "id",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        "days_since_hire_date",
        (F.col("salary") / F.nullif("days_since_hire_date", F.lit(0))).alias("salary_per_days_since_hire_date"),
        F.upper("full_name").alias("full_name_upper"),
        F.coalesce("level", F.lit("NONE")).alias("level_filled"),
    )
)

filtered = (
    valid
    .filter((F.col("hire_date") >= F.lit("2023-09-02").cast("date")) & F.col("job_title").isNotNull())
    .select(
        "id",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        "days_since_hire_date",
        "salary_per_days_since_hire_date",
        "full_name_upper",
        "level_filled",
        F.lower("level").alias("level_lower"),
    )
)

scored = (
    filtered
    .filter(F.col("level") == "L5")
    .select(
        "id",
        "job_title",
        "hire_date",
        "level",
        "salary_per_days_since_hire_date",
        "full_name_upper",
        "level_lower",
        F.ntile(5).over(w).alias("salary_bucket"),
        F.round((F.col("salary") / F.sum("salary").over(w2)), 4).alias("salary_share"),
    )
)

result = (
    scored
    .groupBy("job_title")
    .pivot("level", ["L4", "L3"])
    .agg(F.max("salary_share"))
)
