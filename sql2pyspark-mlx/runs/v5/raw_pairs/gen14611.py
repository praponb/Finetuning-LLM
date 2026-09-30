from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level", "manager_id").orderBy(F.col("hire_date").desc())
w2 = Window.partitionBy("manager_id").orderBy("full_name_length", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("level", "dept_id").orderBy("hire_date", "id").rowsBetween(-6, -1)
w4 = Window.partitionBy("dept_id", "manager_id").orderBy(F.col("full_name_length").desc(), "id")
w5 = Window.partitionBy("job_title").orderBy("rank_in_group", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("dept_id").orderBy(F.col("full_name_length").asc_nulls_last(), "id")
w_recent = Window.partitionBy("job_title").orderBy("first_known_full_name_length", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
salary_by_manager_id = df_employees.groupBy("manager_id").agg(F.max("salary").alias("max_employee_salary"))

filtered = (
    df_employees
    .filter(F.col("level").isNotNull() & (F.col("salary") >= 147000))
    .select("*", F.length("full_name").alias("full_name_length"))
)

ranked = (
    filtered
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
)

valid = (
    ranked
    .filter(F.col("hire_date") >= F.lit("2023-03-14").cast("date"))
    .withColumn("full_name_normalized", F.upper(F.trim("full_name")))
)

scored = (
    valid
    .select(
        "*",
        F.last("job_title", ignorenulls=True).over(w2).alias("last_known_job_title"),
        F.avg("full_name_length").over(w3).alias("full_name_length_avg_6_prior"),
        F.ntile(4).over(w4).alias("full_name_length_bucket"),
        F.first("full_name_length", ignorenulls=True).over(w5).alias("first_known_full_name_length"),
    )
)

ranked_employees = (
    scored
    .withColumn("row_num", F.row_number().over(w6))
    .filter(F.col("row_num") == 1)
)

with_windows = (
    ranked_employees
    .select(
        "id",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "full_name_length",
        "last_known_job_title",
        "full_name_length_bucket",
        "row_num",
        F.count("full_name_length_avg_6_prior").over(w_recent).alias("running_count_full_name_length_avg_6_prior"),
        F.sum("full_name_length_avg_6_prior").over(w_recent).alias("running_sum_full_name_length_avg_6_prior"),
        F.min("full_name_length_avg_6_prior").over(w_recent).alias("running_min_full_name_length_avg_6_prior"),
    )
)

summary = (
    with_windows
    .groupingSets(
        [["job_title", "manager_id"], ["job_title"], []],
        "job_title",
        "manager_id",
    )
    .agg(
        F.percentile_approx("full_name_length_bucket", [0.25, 0.5, 0.75]).alias("full_name_length_bucket_quartiles"),
        F.sum(F.when(F.col("job_title") == "Intern", F.col("running_count_full_name_length_avg_6_prior")).otherwise(0)).alias("intern_running_count_full_name_length_avg_6_prior"),
        F.percentile_approx("running_sum_full_name_length_avg_6_prior", 0.5).alias("running_sum_full_name_length_avg_6_prior_p50"),
        F.count(F.when(F.col("job_title") == "Director", 1)).alias("director_count"),
        F.count(F.when(F.col("job_title") == "Engineer", 1)).alias("engineer_count"),
    )
)

result = (
    summary
    .join(salary_by_manager_id, on="manager_id", how="left")
    .select(
        "job_title",
        "manager_id",
        "full_name_length_bucket_quartiles",
        "intern_running_count_full_name_length_avg_6_prior",
        "running_sum_full_name_length_avg_6_prior_p50",
        "director_count",
        "engineer_count",
        F.col("max_employee_salary"),
    )
)
