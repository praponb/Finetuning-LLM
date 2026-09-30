from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title")
w2 = Window.partitionBy("dept_id")

kept_keys = (
    df_employees
    .select("id", "job_title")
    .subtract(
        df_employees
        .filter(F.col("salary") > 121000)
        .select("id", "job_title")
    )
)

kept_rows = (
    df_employees
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.job_title") == F.col("k.job_title")), "inner")
    .select("s.*")
)

scored = (
    kept_rows
    .select(
        "*",
        F.round((F.col("salary") / F.sum("salary").over(w)), 4).alias("salary_share"),
        F.round((F.col("salary") / F.sum("salary").over(w2)), 4).alias("salary_share_2"),
        F.round((F.col("salary") / F.sum("salary").over(w)), 4).alias("salary_share_3"),
    )
)

matched_keys = (
    scored
    .select("job_title", "manager_id")
    .subtract(
        df_employees
        .filter(F.col("hire_date") <= "2025-08-01")
        .select("job_title", "manager_id")
    )
)

verified = (
    scored
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.job_title") == F.col("k.job_title")) & (F.col("s.manager_id") == F.col("k.manager_id")), "inner")
    .select("s.*")
)

with_dims = (
    verified
    .alias("s")
    .join(df_employees.alias("p"), F.col("s.manager_id") == F.col("p.id"), "left")
    .select(
        "s.*",
        F.col("p.hire_date").alias("employee_hire_date"),
        F.col("p.salary").alias("employee_salary"),
    )
)

totals = (
    with_dims
    .withColumn("year_start", F.date_trunc("YEAR", "employee_hire_date"))
    .withColumn("salary_share_3_tier", F.when(F.col("salary_share_3") >= 0.69, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["year_start", "salary_share_3_tier"], ["year_start"]],
        "year_start",
        "salary_share_3_tier",
    )
    .agg(
        F.countDistinct("job_title").alias("distinct_job_titles"),
        F.size(F.collect_set("job_title")).alias("num_job_titles"),
        F.percentile_approx("salary_share", [0.25, 0.5, 0.75]).alias("salary_share_quartiles"),
        F.count_if(F.col("job_title") == "Designer").alias("designer_events"),
        F.avg("salary").alias("avg_salary"),
        F.percentile_approx("salary", 0.95).alias("salary_p95"),
        F.grouping_id().alias("gid"),
    )
)

result = (
    totals
    .select(
        "year_start",
        "salary_share_3_tier",
        "distinct_job_titles",
        "num_job_titles",
        "salary_share_quartiles",
        "avg_salary",
        "salary_p95",
        "gid",
    )
    .orderBy(F.col("year_start").desc())
)
