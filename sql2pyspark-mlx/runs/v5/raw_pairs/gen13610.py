from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("employee_job_title").orderBy("employee_hire_date", "id")
w2 = Window.partitionBy("employee_job_title")
w3 = Window.partitionBy("manager_id", "employee_job_title").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_employees
    .filter(F.col("level").isNotNull())
    .select(
        "id",
        "full_name",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        F.regexp_replace("email", " ", "-").alias("email_slug"),
        F.col("is_remote").cast("int").alias("is_remote_flag"),
        F.coalesce("job_title", F.lit("UNKNOWN")).alias("job_title_clean"),
    )
)

joined = (
    filtered
    .alias("e")
    .join(df_employees.alias("p2"), F.col("e.manager_id") == F.col("p2.id"), "inner")
    .filter(F.upper(F.trim(F.col("p2.email"))) == "LIAM@OUTLOOK.COM")
    .select(
        "e.id",
        "e.full_name",
        "e.manager_id",
        "e.salary",
        "e.hire_date",
        "e.is_remote",
        "e.email_slug",
        F.col("p2.job_title").alias("employee_job_title"),
        F.col("p2.hire_date").alias("employee_hire_date"),
        F.col("p2.salary").alias("employee_salary"),
    )
)

metrics = (
    joined
    .withColumn("next_employee_salary", F.lead("employee_salary", 1, 0).over(w))
    .withColumn("employee_job_title_max_salary", F.max("salary").over(w2))
)

scored = (
    metrics
    .select(
        "id",
        "salary",
        "is_remote",
        "employee_salary",
        "next_employee_salary",
        "employee_job_title_max_salary",
        F.min("employee_salary").over(w3).alias("running_min_employee_salary"),
        F.avg("employee_salary").over(w3).alias("running_avg_employee_salary"),
        F.sum("employee_salary").over(w3).alias("running_sum_employee_salary"),
        F.max("employee_salary").over(w3).alias("running_max_employee_salary"),
    )
)

result = (
    scored
    .groupBy(
        F.when(F.col("salary") >= 121000, "LARGE").otherwise("SMALL").alias("salary_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("id").alias("distinct_ids_2"),
        F.percentile_approx("next_employee_salary", 0.95).alias("next_employee_salary_p95"),
        F.count("*").alias("cnt"),
        F.percentile_approx("salary", 0.5).alias("salary_p50"),
    )
    .filter(F.col("distinct_ids_2") > 1)
)
