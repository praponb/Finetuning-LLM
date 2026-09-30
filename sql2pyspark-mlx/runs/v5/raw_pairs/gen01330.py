from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("manager_id", "dept_id").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("job_title", "manager_id").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_employees
    .filter((F.col("hire_date") >= F.lit("2023-05-07").cast("date")) & F.col("level").isNotNull())
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "hire_date",
        "is_remote",
        "level",
        F.trim("level").alias("clean_level"),
        F.round("salary", 2).alias("salary_rounded"),
    )
)

enriched = (
    base
    .select(
        "id",
        "email",
        "dept_id",
        "hire_date",
        "is_remote",
        "level",
        "clean_level",
        F.count("salary_rounded").over(w_grp).alias("running_count_salary_rounded"),
        F.max("salary_rounded").over(w_grp).alias("running_max_salary_rounded"),
        F.sum("salary_rounded").over(w_grp).alias("running_sum_salary_rounded"),
        F.avg("salary_rounded").over(w_grp).alias("running_avg_salary_rounded"),
        F.first("job_title", ignorenulls=True).over(w_part).alias("first_known_job_title"),
    )
)

matched_keys = (
    enriched
    .select("level", "id")
    .subtract(
        df_employees
        .filter(F.col("dept_id").isin([27, 38]))
        .select("level", "id")
    )
)

kept_rows = (
    enriched
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.level") == F.col("k.level")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

filtered = (
    kept_rows
    .filter(F.col("running_count_salary_rounded").between(6, 9))
    .withColumn("running_sum_salary_rounded_rounded", F.round("running_sum_salary_rounded", 1))
    .withColumn("level_clean", F.coalesce("level", F.lit("UNKNOWN")))
)

cleaned = (
    filtered
    .filter((F.col("hire_date") >= F.lit("2023-05-30").cast("date")) & (~F.col("level").eqNullSafe(F.lit("L2"))))
    .withColumn("adjusted_running_sum_salary_rounded", F.round((F.col("running_sum_salary_rounded") * 1.07), 2))
    .withColumn("level_filled", F.coalesce("level", F.lit("OTHER")))
)

result = (
    cleaned
    .select(
        "dept_id",
        "hire_date",
        "clean_level",
        "running_count_salary_rounded",
        "running_max_salary_rounded",
        "running_sum_salary_rounded",
        "first_known_job_title",
        "running_sum_salary_rounded_rounded",
        "level_clean",
    )
    .orderBy(F.col("running_sum_salary_rounded").desc_nulls_first())
)
