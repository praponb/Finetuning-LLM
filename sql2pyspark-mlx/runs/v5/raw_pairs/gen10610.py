from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy(F.col("hire_date").desc_nulls_last(), "id")
w2 = Window.partitionBy("manager_id").orderBy(F.col("hire_date").desc(), "id")

base = (
    df_employees
    .filter(F.col("job_title").isNotNull() & F.col("full_name").isNotNull())
    .select(
        "id",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "hire_date",
        "is_remote",
        "level",
        F.lower("level").alias("level_lower"),
        F.coalesce("job_title", F.lit("UNKNOWN")).alias("resolved_job_title"),
    )
)

latest = (
    base
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "dept_id", "manager_id", "hire_date", "level", "level_lower")
)

latest_employees = (
    latest
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

joined = (
    latest_employees
    .alias("src")
    .join(df_employees.alias("b"), F.col("src.manager_id") == F.col("b.id"), "left")
    .select(
        "src.id",
        "src.manager_id",
        "src.hire_date",
        "src.level_lower",
        F.col("b.level").alias("employee_level"),
        F.col("b.job_title"),
        F.col("b.full_name"),
    )
)

result = (
    joined
    .withColumn("year_start", F.date_trunc("YEAR", "hire_date"))
    .rollup("year_start", "job_title")
    .agg(
        F.count(F.when(F.col("job_title") == "Director", 1)).alias("director_rows"),
        F.count(F.when(F.col("job_title") == "Manager", 1)).alias("manager_count"),
        F.count("*").alias("event_count"),
        F.count_if(F.col("employee_level") == "L5").alias("l5_events"),
        F.grouping_id().alias("grouping_level"),
    )
)
