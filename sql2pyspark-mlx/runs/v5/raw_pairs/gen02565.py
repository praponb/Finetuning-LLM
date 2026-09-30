from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("manager_id").orderBy("salary", "id")
w2 = Window.orderBy("salary", "id")
w3 = Window.partitionBy("dept_id").orderBy(F.col("salary_bucket_minus_next_salary").desc_nulls_first(), "id")
w4 = Window.partitionBy("dept_id").orderBy("salary_bucket_plus_salary", "id")

with_windows = (
    df_employees
    .select(
        "id",
        "full_name",
        "dept_id",
        "job_title",
        "salary",
        "hire_date",
        F.lag("salary").over(w_grp).alias("prev_salary"),
        F.lead("salary").over(w_grp).alias("next_salary"),
        F.row_number().over(w_grp).alias("row_seq"),
        F.ntile(5).over(w2).alias("salary_bucket"),
    )
)

filtered = (
    with_windows
    .filter(F.col("prev_salary") < 138000)
    .select(
        "id",
        "dept_id",
        "salary",
        "hire_date",
        "next_salary",
        "row_seq",
        (F.col("salary_bucket") - F.col("next_salary")).alias("salary_bucket_minus_next_salary"),
        (F.col("salary_bucket") + F.col("salary")).alias("salary_bucket_plus_salary"),
        F.coalesce("job_title", F.lit("OTHER")).alias("job_title_filled"),
    )
)

ranked = (
    filtered
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

metrics = (
    ranked
    .withColumn("prev_salary", F.lag("salary").over(w4))
    .withColumn("next_row_seq", F.lead("row_seq").over(w4))
    .withColumn("seq_no", F.row_number().over(w4))
)

filtered_employees = (
    metrics
    .filter(F.col("seq_no") < 6)
    .withColumn("days_since_hire_date", F.datediff(F.current_date(), "hire_date"))
)

result = (
    filtered_employees
    .select(
        "id",
        "dept_id",
        "salary",
        "hire_date",
        "next_salary",
        "row_seq",
        "salary_bucket_minus_next_salary",
        "prev_salary",
        "seq_no",
    )
    .orderBy(F.col("hire_date").desc())
)
