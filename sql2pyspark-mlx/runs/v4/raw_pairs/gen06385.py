from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy(F.col("salary").desc())
w_hist = Window.partitionBy("job_title").orderBy("salary", "id")

valid = (
    df_employees
    .filter(F.col("job_title").isNotNull())
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        F.lower("job_title").alias("job_title_lower"),
    )
)

with_windows = (
    valid
    .select(
        "*",
        F.cume_dist().over(w).alias("salary_cume_dist"),
        F.row_number().over(w_hist).alias("seq_no"),
    )
)

matched_keys = (
    with_windows
    .select("manager_id")
    .subtract(
        df_employees
        .filter(F.col("level").isin(["L3", "L5", "L4"]))
        .select("manager_id")
    )
)

verified = (
    with_windows
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.manager_id") == F.col("k.manager_id")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy(
        F.when(F.col("salary") >= 157000, "LARGE").otherwise("SMALL").alias("salary_tier"),
    )
    .agg(
        F.percentile_approx("seq_no", [0.25, 0.5, 0.75]).alias("seq_no_quartiles"),
        F.avg("salary_cume_dist").alias("avg_salary_cume_dist"),
        F.count("*").alias("row_count"),
        F.percentile_approx("seq_no", [0.25, 0.5, 0.75]).alias("seq_no_quartiles_2"),
        F.count(F.when(F.col("job_title") == "Intern", 1)).alias("intern_count"),
        F.count("*").alias("event_count"),
    )
    .orderBy(F.col("salary_tier").desc_nulls_last())
)
