from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title")
w2 = Window.partitionBy("level").orderBy(F.col("salary_rounded").desc(), "id")
w3 = Window.partitionBy("dept_id")
w_recent = Window.partitionBy("job_title", "dept_id").orderBy(F.col("salary_rounded").desc_nulls_first())
w5 = Window.partitionBy("manager_id", "job_title")
w6 = Window.partitionBy("job_title").orderBy("seq_no", "id")
w7 = Window.orderBy("seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.orderBy("dept_id_avg_salary", "id").rowsBetween(-3, -1)
w9 = Window.partitionBy("job_title").orderBy(F.col("salary_share").asc_nulls_first())
w10 = Window.orderBy("cumulative_dept_id_avg_salary", "id")
w11 = Window.partitionBy("job_title").orderBy(F.col("salary_rounded_percent_rank").desc(), "id")

valid = (
    df_employees
    .filter(F.col("hire_date") > F.lit("2023-09-03").cast("date"))
    .withColumn("job_title_level", F.concat("job_title", F.lit("/"), "level"))
    .withColumn("salary_rounded", F.round("salary", 2))
)

scored = (
    valid
    .withColumn("job_title_stddev_samp_salary_rounded", F.stddev_samp("salary_rounded").over(w))
    .withColumn("seq_no", F.row_number().over(w2))
    .withColumn("dept_id_avg_salary", F.avg("salary").over(w3))
    .withColumn("salary_rounded_percent_rank", F.percent_rank().over(w_recent))
)

cleaned = (
    scored
    .filter((F.col("hire_date") > F.lit("2023-11-01").cast("date")) & F.col("job_title").isNotNull())
    .select(
        "id",
        "full_name",
        "email",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        "job_title_level",
        "job_title_stddev_samp_salary_rounded",
        "seq_no",
        "dept_id_avg_salary",
        "salary_rounded_percent_rank",
        (F.col("job_title_stddev_samp_salary_rounded") + F.col("dept_id_avg_salary")).alias("job_title_stddev_samp_salary_rounded_plus_dept_id_avg_salary"),
    )
)

metrics = (
    cleaned
    .select(
        "*",
        F.round((F.col("salary") / F.sum("salary").over(w5)), 4).alias("salary_share"),
        F.lag("seq_no", 1).over(w6).alias("prev_seq_no"),
        F.sum("dept_id_avg_salary").over(w7).alias("cumulative_dept_id_avg_salary"),
        F.avg("salary_rounded_percent_rank").over(w8).alias("salary_rounded_percent_rank_avg_3_prior"),
        F.round((F.col("seq_no") / F.sum("seq_no").over(w5)), 4).alias("seq_no_share"),
    )
)

result = (
    metrics
    .select(
        "*",
        F.dense_rank().over(w9).alias("salary_share_dense_rank"),
        F.ntile(3).over(w10).alias("cumulative_dept_id_avg_salary_bucket"),
        F.row_number().over(w11).alias("event_seq"),
    )
)
