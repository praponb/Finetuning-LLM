from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("dept_id").orderBy(F.col("salary").desc())
w2 = Window.partitionBy("dept_id")

valid = (
    df_employees
    .filter((F.col("hire_date") > F.lit("2023-03-18").cast("date")) & (F.col("hire_date") < "2023-05-01"))
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "job_title",
        "salary",
        F.when(F.col("is_remote"), "Yes").otherwise("No").alias("is_remote_label"),
        F.coalesce("job_title", F.lit("UNKNOWN")).alias("job_title_clean"),
    )
)

enriched = (
    valid
    .alias("src")
    .join(df_departments.alias("b"), F.col("src.dept_id") == F.col("b.id"), "left")
    .select("src.*", F.col("b.dept_name"))
)

matched_keys = (
    enriched
    .select("dept_id", "id")
    .intersect(
        df_employees
        .filter(F.col("job_title").isNotNull())
        .select("dept_id", "id")
    )
)

kept_rows = (
    enriched
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.dept_id") == F.col("k.dept_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

with_windows = (
    kept_rows
    .withColumn("salary_cume_dist", F.cume_dist().over(w_part))
    .withColumn("dept_id_max_salary", F.max("salary").over(w2))
)

enriched_employees = (
    with_windows
    .alias("src")
    .join(df_departments.alias("p"), F.col("src.dept_id") == F.col("p.id"), "inner")
    .select(
        "src.*",
        F.col("p.location"),
        F.col("p.budget"),
        F.col("p.dept_name").alias("department_dept_name"),
    )
)

result = (
    enriched_employees
    .select(
        "id",
        "email",
        "dept_id",
        "job_title",
        "salary",
        "is_remote_label",
        "job_title_clean",
        "dept_name",
        "dept_id_max_salary",
    )
    .orderBy(F.col("id").desc_nulls_last(), F.col("job_title").desc())
)
