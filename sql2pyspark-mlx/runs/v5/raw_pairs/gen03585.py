from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id", "dept_id").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("dept_id").orderBy("hire_date", "id").rowsBetween(-5, -1)
w3 = Window.partitionBy("job_title", "level").orderBy("running_sum_salary", "id").rowsBetween(-6, Window.currentRow)
w4 = Window.partitionBy("manager_id")
w_part = Window.partitionBy("job_title", "dept_id").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("job_title").orderBy("running_min_salary_avg_6", "id").rowsBetween(-6, Window.currentRow)

cleaned = (
    df_employees
    .filter(F.col("hire_date") < "2024-12-01")
    .select(
        "*",
        F.lower("full_name").alias("full_name_lower"),
        F.coalesce("job_title", F.lit("UNKNOWN")).alias("job_title_filled"),
    )
)

enriched = (
    cleaned
    .filter(F.col("email").like("%@outlook.com"))
    .withColumn("running_min_salary", F.min("salary").over(w))
    .withColumn("running_max_salary", F.max("salary").over(w))
    .withColumn("running_sum_salary", F.sum("salary").over(w))
)

scored = (
    enriched
    .select(
        "id",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "is_remote",
        "full_name_lower",
        "running_max_salary",
        "running_sum_salary",
        F.avg("running_max_salary").over(w_grp).alias("running_max_salary_avg_5_prior"),
        F.avg("running_min_salary").over(w3).alias("running_min_salary_avg_6"),
    )
)

scored_employees = (
    scored
    .select(
        "id",
        "manager_id",
        "is_remote",
        "full_name_lower",
        "running_sum_salary",
        "running_max_salary_avg_5_prior",
        F.stddev_samp("running_max_salary").over(w4).alias("manager_id_stddev_samp_running_max_salary"),
        F.sum("running_sum_salary").over(w_part).alias("running_running_sum_salary"),
        F.avg("running_min_salary_avg_6").over(w6).alias("running_min_salary_avg_6_avg_6"),
    )
)

result = (
    scored_employees
    .select(
        "id",
        "is_remote",
        "running_sum_salary",
        "running_max_salary_avg_5_prior",
        "running_running_sum_salary",
    )
    .orderBy(F.col("id").desc())
    .limit(20)
)
