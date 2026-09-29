from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("level").orderBy("hire_date", "id").rowsBetween(-2, Window.currentRow)
w_part_2 = Window.orderBy("salary", "id")
w3 = Window.partitionBy("manager_id")
w4 = Window.partitionBy("level").orderBy("last3_max_salary", "id")

filtered = (
    df_employees
    .filter(F.col("manager_id").isNotNull() & F.col("manager_id").isin([1, 36]))
    .withColumn("full_name_job_title", F.concat("full_name", F.lit("/"), "job_title"))
    .withColumn("job_title_group", F.when(F.col("job_title").isin(["Engineer", "Designer"]), "Group A").otherwise("Other"))
    .withColumn("resolved_job_title", F.coalesce("job_title", F.lit("NONE")))
)

with_windows = (
    filtered
    .filter(F.col("job_title") == "Engineer")
    .select(
        "id",
        "manager_id",
        "job_title",
        "hire_date",
        "is_remote",
        "level",
        "full_name_job_title",
        F.avg("salary").over(w_part).alias("last3_avg_salary"),
        F.count("salary").over(w_part).alias("last3_count_salary"),
        F.max("salary").over(w_part).alias("last3_max_salary"),
        F.ntile(3).over(w_part_2).alias("salary_bucket"),
    )
)

cleaned = (
    with_windows
    .filter((F.col("hire_date") > F.to_date(F.lit("2023-05-12"))) & (F.col("job_title").eqNullSafe(F.lit("Manager"))))
    .select(
        "*",
        F.length("job_title").alias("job_title_length"),
        F.when(F.col("job_title") == "Engineer", 1).otherwise(0).alias("is_engineer"),
        F.coalesce("job_title", F.lit("N/A")).alias("job_title_clean"),
    )
)

enriched = (
    cleaned
    .withColumn("manager_id_min_is_engineer", F.min("is_engineer").over(w3))
    .withColumn("last3_max_salary_bucket", F.ntile(3).over(w4))
)

with_dims = (
    enriched
    .alias("src")
    .join(df_employees.alias("d"), F.col("src.manager_id").eqNullSafe(F.col("d.id")), "left")
    .select("src.*", F.col("d.email"), F.col("d.hire_date").alias("employee_hire_date"))
)

result = (
    with_dims
    .withColumn("activity_month", F.date_trunc("MONTH", "employee_hire_date"))
    .groupingSets(
        [["activity_month", "level"], ["activity_month"]],
        "activity_month",
        "level",
    )
    .agg(
        F.round(F.avg("last3_max_salary_bucket"), 2).alias("avg_last3_max_salary_bucket"),
        F.percentile_approx("last3_avg_salary", 0.95).alias("last3_avg_salary_p95"),
        F.count(F.when(F.col("job_title") == "Director", 1)).alias("director_count"),
    )
)
