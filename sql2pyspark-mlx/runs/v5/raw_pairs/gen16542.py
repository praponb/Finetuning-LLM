from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id", "level").orderBy("salary", "id")
w2 = Window.partitionBy("job_title", "dept_id").orderBy(F.col("next_salary").desc_nulls_last())
w3 = Window.partitionBy("manager_id", "job_title")

enriched = (
    df_employees
    .filter(F.col("is_remote"))
    .select(
        "*",
        F.lead("salary").over(w).alias("next_salary"),
        F.row_number().over(w).alias("seq_no"),
    )
)

matched_keys = (
    enriched
    .select("level", "id")
    .intersectAll(
        df_employees
        .filter(F.col("dept_id").isin([22, 32, 33]))
        .select("level", "id")
    )
)

verified = (
    enriched
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.level") == F.col("k.level")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

valid = (
    verified
    .filter((F.col("hire_date") > F.to_date(F.lit("2023-09-07"))) & F.col("level").isNotNull())
    .select(
        "id",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        "next_salary",
        "seq_no",
        F.when(F.col("is_remote"), "Yes").otherwise("No").alias("is_remote_label"),
    )
)

joined = (
    valid
    .alias("s")
    .join(df_employees.alias("b"), F.col("s.manager_id") == F.col("b.id"), "left")
    .select(
        "s.*",
        F.col("b.level").alias("employee_level"),
        F.col("b.salary").alias("employee_salary"),
    )
)

with_windows = (
    joined
    .withColumn("next_salary_dense_rank", F.dense_rank().over(w2))
    .withColumn("manager_id_stddev_samp_employee_salary", F.stddev_samp("employee_salary").over(w3))
)

joined_employees = (
    with_windows
    .alias("src")
    .join(df_departments.alias("p"), F.col("src.dept_id") == F.col("p.id"), "left")
    .select(
        "src.email",
        "src.dept_id",
        "src.manager_id",
        "src.job_title",
        "src.salary",
        "src.hire_date",
        "src.is_remote",
        "src.seq_no",
        "src.is_remote_label",
        "src.employee_salary",
        "src.next_salary_dense_rank",
        "src.manager_id_stddev_samp_employee_salary",
        F.col("p.dept_name"),
    )
)

valid_employees = (
    joined_employees
    .filter(F.col("manager_id_stddev_samp_employee_salary").isNotNull() & F.col("dept_name").isNotNull())
    .select(
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        "seq_no",
        "is_remote_label",
        "next_salary_dense_rank",
        F.round("manager_id_stddev_samp_employee_salary", 0).alias("manager_id_stddev_samp_employee_salary_rounded"),
    )
)

result = (
    valid_employees
    .groupBy(
        F.date_trunc("year", "hire_date").alias("year_start"),
        F.when(F.col("seq_no") >= 8, "LARGE").otherwise("SMALL").alias("seq_no_tier"),
        "job_title",
    )
    .agg(
        F.count(F.when(F.col("job_title") == "Designer", 1)).alias("designer_rows"),
        F.percentile_approx("manager_id_stddev_samp_employee_salary_rounded", [0.25, 0.5, 0.75]).alias("manager_id_stddev_samp_employee_salary_rounded_quartiles"),
        F.round(F.avg("salary"), 2).alias("avg_salary"),
    )
    .filter(F.col("designer_rows") < 1000)
    .orderBy(F.col("year_start").asc_nulls_first())
)
