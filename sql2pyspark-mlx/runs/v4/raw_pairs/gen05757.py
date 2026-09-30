from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id", "dept_id").orderBy("salary", "id")
w2 = Window.partitionBy("level").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("level", "manager_id").orderBy("is_remote_flag", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("job_title").orderBy("salary", "id")

cleaned = (
    df_employees
    .filter((F.col("hire_date") > F.lit("2023-09-18").cast("date")) & F.col("level").isNotNull())
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "level",
        F.initcap(F.trim("job_title")).alias("job_title_title"),
        F.col("is_remote").cast("int").alias("is_remote_flag"),
        F.coalesce("job_title", F.lit("NONE")).alias("job_title_clean"),
    )
)

with_windows = (
    cleaned
    .withColumn("salary_bucket", F.ntile(5).over(w))
    .withColumn("last_known_job_title", F.last("job_title", ignorenulls=True).over(w2))
    .withColumn("running_is_remote_flag", F.sum("is_remote_flag").over(w3))
)

with_dims = (
    with_windows
    .alias("src")
    .join(df_departments.alias("d"), F.col("src.dept_id") == F.col("d.id"), "inner")
    .select("src.*", F.col("d.dept_name"), F.col("d.budget"), F.col("d.location"))
)

deduped = (
    with_dims
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
)

filtered = (
    deduped
    .filter(F.col("level").isNotNull())
    .select(
        "id",
        "full_name",
        "email",
        "manager_id",
        "job_title",
        "level",
        "job_title_title",
        "job_title_clean",
        "salary_bucket",
        "running_is_remote_flag",
        "dept_name",
        "budget",
        "location",
        "rn",
        F.concat("full_name", F.lit(", "), "level").alias("full_name_level"),
        F.lower("email").alias("email_lower"),
    )
)

result = (
    filtered
    .groupBy("job_title")
    .agg(
        F.avg(F.when(F.col("dept_name") == "Marketing", F.col("salary_bucket"))).alias("marketing_avg_salary_bucket"),
        F.count(F.when(F.col("dept_name") == "Sales", 1)).alias("sales_count"),
        F.percentile_approx("salary_bucket", [0.25, 0.5, 0.75]).alias("salary_bucket_quartiles"),
    )
)
