from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("level", "manager_id").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("dept_id").orderBy("hire_date", "id")
w3 = Window.partitionBy("manager_id")
w4 = Window.partitionBy("job_title").orderBy(F.col("running_min_salary").desc(), "id")
w5 = Window.partitionBy("manager_id").orderBy("seq_no", "id")
w6 = Window.partitionBy("level").orderBy(F.col("running_min_salary_bucket").desc_nulls_first(), "id")
w7 = Window.partitionBy("level").orderBy("seq_no", "id")
w8 = Window.orderBy("seq_no", "id").rowsBetween(-5, -1)
w9 = Window.partitionBy("level").orderBy("prev_manager_id_avg_job_title_length", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_employees
    .filter((F.col("hire_date") > F.to_date(F.lit("2023-08-12"))) & F.col("manager_id").isNotNull())
    .select(
        "*",
        F.datediff(F.current_date(), "hire_date").alias("days_since_hire_date"),
        F.length("job_title").alias("job_title_length"),
        F.coalesce("job_title", F.lit("N/A")).alias("job_title_clean"),
    )
)

scored = (
    valid
    .select(
        "id",
        "full_name",
        "email",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        "level",
        "job_title_length",
        F.sum("salary").over(w_running).alias("running_sum_salary"),
        F.count("salary").over(w_running).alias("running_count_salary"),
        F.min("salary").over(w_running).alias("running_min_salary"),
        F.row_number().over(w2).alias("seq_no"),
    )
)

metrics = (
    scored
    .select(
        "id",
        "full_name",
        "email",
        "manager_id",
        "job_title",
        "salary",
        "level",
        "running_count_salary",
        "seq_no",
        F.avg("job_title_length").over(w3).alias("manager_id_avg_job_title_length"),
        F.ntile(3).over(w4).alias("running_min_salary_bucket"),
        F.ntile(10).over(w4).alias("running_min_salary_bucket_2"),
    )
)

metrics_employees = (
    metrics
    .filter(F.col("running_min_salary_bucket") >= 4)
    .select(
        "id",
        "email",
        "salary",
        "level",
        "seq_no",
        "manager_id_avg_job_title_length",
        "running_min_salary_bucket",
        "running_min_salary_bucket_2",
        F.lag("manager_id_avg_job_title_length").over(w5).alias("prev_manager_id_avg_job_title_length"),
        F.lead("manager_id_avg_job_title_length").over(w5).alias("next_manager_id_avg_job_title_length"),
        F.lag("seq_no").over(w5).alias("prev_seq_no"),
        F.lead("seq_no").over(w5).alias("next_seq_no"),
        F.row_number().over(w5).alias("row_seq"),
    )
)

deduped = (
    metrics_employees
    .withColumn("dedup_rank", F.row_number().over(w6))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    deduped
    .select(
        "id",
        "email",
        "salary",
        "running_min_salary_bucket_2",
        "next_manager_id_avg_job_title_length",
        "next_seq_no",
        "row_seq",
        F.ntile(3).over(w7).alias("seq_no_bucket"),
        F.avg("row_seq").over(w8).alias("row_seq_avg_5_prior"),
        F.sum("running_min_salary_bucket_2").over(w9).alias("cumulative_running_min_salary_bucket_2"),
    )
)
