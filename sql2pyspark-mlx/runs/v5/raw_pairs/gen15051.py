from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("dept_id").orderBy("hire_date", "id")
w_part = Window.partitionBy("job_title").orderBy("hire_date", "id")
w4 = Window.partitionBy("dept_id", "job_title").orderBy(F.col("seq_no").desc_nulls_first(), "id")
w5 = Window.partitionBy("job_title").orderBy(F.col("seq_no_2").desc(), "id")
w6 = Window.partitionBy("job_title", "dept_id").orderBy("seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("dept_name").orderBy("seq_no_2", "id")
w8 = Window.orderBy(F.col("seq_no_2").desc())
w9 = Window.partitionBy("dept_name").orderBy("hire_date", "id")
w_ordered_2 = Window.orderBy("seq_no", "id")
w_running = Window.partitionBy("dept_name", "dept_id").orderBy("seq_no_2", "id")

cleaned = (
    df_employees
    .filter(F.col("manager_id").isNotNull() & (~F.col("level").eqNullSafe(F.lit("L3"))))
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "job_title",
        "hire_date",
        "is_remote",
        F.initcap(F.trim("email")).alias("email_title"),
        F.coalesce("job_title", F.lit("OTHER")).alias("job_title_clean"),
    )
)

enriched = (
    cleaned
    .withColumn("first_known_job_title", F.first("job_title", ignorenulls=True).over(w))
)

with_windows = (
    enriched
    .select(
        "id",
        "full_name",
        "dept_id",
        "job_title",
        "hire_date",
        "is_remote",
        F.last("job_title", ignorenulls=True).over(w).alias("last_known_job_title"),
        F.row_number().over(w_ordered).alias("seq_no"),
        F.row_number().over(w_ordered).alias("seq_no_2"),
        F.row_number().over(w_part).alias("row_seq"),
    )
)

deduped = (
    with_windows
    .withColumn("dedup_rank", F.row_number().over(w4))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

enriched_employees = (
    deduped
    .alias("a")
    .join(df_departments.alias("p"), F.col("a.dept_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.dept_name"))
)

enriched_employees_2 = (
    enriched_employees
    .select(
        "*",
        F.ntile(5).over(w5).alias("seq_no_2_bucket"),
        F.first("dept_name", ignorenulls=True).over(w6).alias("first_known_dept_name"),
        F.ntile(4).over(w7).alias("seq_no_2_bucket_2"),
    )
)

scored = (
    enriched_employees_2
    .select(
        "*",
        F.cume_dist().over(w8).alias("seq_no_2_cume_dist"),
        F.lag("seq_no_2_bucket", 1).over(w9).alias("prev_seq_no_2_bucket"),
        F.lag("seq_no", 1).over(w_ordered_2).alias("prev_seq_no"),
        F.lead("seq_no", 1, 0).over(w_running).alias("next_seq_no"),
    )
)

result = (
    scored
    .filter(F.col("seq_no_2_bucket_2").between(7, 8))
    .withColumn("next_seq_no_2", F.lead("seq_no_2").over(w7))
    .withColumn("prev_seq_no_2", F.lag("seq_no_2").over(w7))
    .withColumn("prev_seq_no_3", F.lag("seq_no").over(w7))
    .withColumn("row_seq_2", F.row_number().over(w7))
)
