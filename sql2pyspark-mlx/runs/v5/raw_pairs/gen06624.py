from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("hire_date_day").desc_nulls_first())
w_grp = Window.partitionBy("job_title").orderBy(F.col("hire_date_day").desc_nulls_last())

cleaned = (
    df_employees
    .filter(F.col("hire_date") >= F.lit("2023-05-30").cast("date"))
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "job_title",
        "is_remote",
        "level",
        F.substring("email", 1, 4).alias("email_prefix"),
        F.dayofmonth("hire_date").alias("hire_date_day"),
    )
)

kept_keys = (
    cleaned
    .select("dept_id")
    .subtract(
        df_employees
        .filter(~F.col("is_remote"))
        .select("dept_id")
    )
)

verified = (
    cleaned
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.dept_id") == F.col("k.dept_id")), "inner")
    .select("s.*")
)

cleaned_employees = (
    verified
    .filter(F.col("job_title").isNotNull())
    .withColumn("level_filled", F.coalesce("level", F.lit("Unknown")))
)

cleaned_employees_2 = (
    cleaned_employees
    .filter(F.col("level").isNotNull())
    .select(
        "id",
        "full_name",
        "job_title",
        "is_remote",
        "hire_date_day",
        "level_filled",
        F.length("level").alias("level_length"),
    )
)

filtered = (
    cleaned_employees_2
    .filter(F.col("job_title") == "Engineer")
    .select(
        "*",
        F.concat("full_name", F.lit(" "), "job_title").alias("full_name_job_title"),
    )
)

enriched = (
    filtered
    .select(
        "id",
        "full_name",
        F.cume_dist().over(w).alias("hire_date_day_cume_dist"),
        F.cume_dist().over(w_grp).alias("hire_date_day_cume_dist_2"),
    )
)

result = (
    enriched
    .select("id", "hire_date_day_cume_dist")
    .orderBy(F.col("id").asc_nulls_last(), F.col("hire_date_day_cume_dist").desc())
)
