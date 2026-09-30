from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy("salary", "id")
w_part = Window.partitionBy("dept_id").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_employees
    .filter((F.col("hire_date") >= F.lit("2023-04-18").cast("date")) & F.col("manager_id").isin([7, 33]))
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "is_remote",
        "level",
        F.quarter("hire_date").alias("hire_date_quarter"),
        F.regexp_replace("email", " ", "").alias("email_slug"),
    )
)

with_dims = (
    filtered
    .alias("a")
    .join(df_departments.alias("b"), F.col("a.dept_id") == F.col("b.id"), "left")
    .filter(F.col("b.location").isin(["Tokyo", "Bangkok"]))
    .select(
        "a.id",
        "a.email",
        "a.dept_id",
        "a.manager_id",
        "a.job_title",
        "a.salary",
        "a.hire_date_quarter",
        "a.email_slug",
        F.col("b.location"),
    )
)

metrics = (
    with_dims
    .select(
        "id",
        "email",
        "dept_id",
        "hire_date_quarter",
        "email_slug",
        "location",
        F.ntile(3).over(w).alias("salary_bucket"),
        F.sum("salary").over(w_part).alias("cumulative_salary"),
    )
)

matched_keys = (
    metrics
    .select("id")
    .intersectAll(
        df_employees
        .filter(F.col("is_remote"))
        .select("id")
    )
)

verified = (
    metrics
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

base = (
    verified
    .filter(F.col("location").isNotNull())
    .select(
        "*",
        F.coalesce("salary_bucket", F.lit(0)).alias("salary_bucket_filled"),
        F.initcap(F.trim("email")).alias("email_title"),
        F.coalesce("location", F.lit("OTHER")).alias("location_clean"),
    )
)

result = (
    base
    .groupBy("location")
    .agg(
        F.round(F.avg("hire_date_quarter"), 2).alias("avg_hire_date_quarter"),
        F.count(F.when(F.col("location") == "Berlin", 1)).alias("berlin_count"),
        F.count(F.when(F.col("location") == "Tokyo", 1)).alias("tokyo_count"),
        F.sort_array(F.collect_set("location")).alias("location_list"),
    )
    .filter(F.col("tokyo_count") > 1)
)
