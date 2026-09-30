from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("dept_id").orderBy("salary", "id")
w3 = Window.partitionBy("dept_id").orderBy(F.col("salary_times_email_length").desc_nulls_last())
w4 = Window.partitionBy("manager_id")
w5 = Window.orderBy(F.col("manager_id_stddev_samp_salary_times_email_length").desc())
w6 = Window.partitionBy("manager_id").orderBy("salary_times_email_length_percent_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.orderBy("manager_id_min_salary_times_email_length_percent_rank", "id")
w8 = Window.partitionBy("manager_id").orderBy("first_known_salary_bucket", "id").rowsBetween(-6, Window.currentRow)

valid = (
    df_employees
    .filter(F.col("manager_id").isNotNull())
    .withColumn("email_length", F.length("email"))
    .withColumn("clean_email", F.trim("email"))
)

base = (
    valid
    .filter((F.col("hire_date") >= F.to_date(F.lit("2023-05-31"))) & F.col("level").isNotNull())
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "salary",
        "is_remote",
        "clean_email",
        (F.col("salary") * F.col("email_length")).alias("salary_times_email_length"),
        F.concat_ws("/", "job_title", "email").alias("job_title_email_key"),
        F.coalesce("level", F.lit("OTHER")).alias("level_clean"),
    )
)

metrics = (
    base
    .select(
        "id",
        "email",
        "manager_id",
        "is_remote",
        "job_title_email_key",
        F.sum("salary_times_email_length").over(w).alias("cumulative_salary_times_email_length"),
        F.ntile(4).over(w2).alias("salary_bucket"),
        F.percent_rank().over(w3).alias("salary_times_email_length_percent_rank"),
        F.stddev_samp("salary_times_email_length").over(w4).alias("manager_id_stddev_samp_salary_times_email_length"),
    )
)

with_windows = (
    metrics
    .filter(F.upper(F.trim("email")) == "BOB.SMITH@YAHOO.COM")
    .select(
        "*",
        F.dense_rank().over(w5).alias("manager_id_stddev_samp_salary_times_email_length_dense_rank"),
        F.first("salary_bucket", ignorenulls=True).over(w6).alias("first_known_salary_bucket"),
        F.min("salary_times_email_length_percent_rank").over(w4).alias("manager_id_min_salary_times_email_length_percent_rank"),
    )
)

enriched = (
    with_windows
    .select(
        "id",
        "email",
        "is_remote",
        "cumulative_salary_times_email_length",
        "salary_times_email_length_percent_rank",
        "first_known_salary_bucket",
        "manager_id_min_salary_times_email_length_percent_rank",
        F.lag("manager_id_stddev_samp_salary_times_email_length", 2).over(w_grp).alias("prev_manager_id_stddev_samp_salary_times_email_length"),
        F.count("manager_id_stddev_samp_salary_times_email_length_dense_rank").over(w4).alias("manager_id_count_manager_id_stddev_samp_salary_times_email_length_dense_rank"),
        F.avg("first_known_salary_bucket").over(w8).alias("first_known_salary_bucket_avg_6"),
    )
)

result = (
    enriched
    .select(
        "id",
        "email",
        "is_remote",
        "salary_times_email_length_percent_rank",
        "first_known_salary_bucket",
        "manager_id_min_salary_times_email_length_percent_rank",
        "prev_manager_id_stddev_samp_salary_times_email_length",
        "manager_id_count_manager_id_stddev_samp_salary_times_email_length_dense_rank",
    )
    .orderBy(
        F.col("prev_manager_id_stddev_samp_salary_times_email_length").asc_nulls_last(),
    )
)
