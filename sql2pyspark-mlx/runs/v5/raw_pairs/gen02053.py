from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("salary", "id")
w2 = Window.partitionBy("dept_id").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("job_title").orderBy("hire_date", "id")
w4 = Window.partitionBy("dept_id").orderBy(F.col("hire_date").desc(), "id")
w_ordered = Window.partitionBy("job_title").orderBy("row_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("job_title").orderBy("row_seq", "id")

latest = (
    df_employees
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "email", "dept_id", "job_title", "hire_date", "is_remote")
)

scored = (
    latest
    .filter(F.col("hire_date") <= "2024-10-01")
    .withColumn("last_known_job_title", F.last("job_title", ignorenulls=True).over(w2))
)

with_windows = (
    scored
    .select(
        "id",
        "job_title",
        F.row_number().over(w3).alias("seq_no"),
        F.row_number().over(w4).alias("row_seq"),
        F.row_number().over(w4).alias("seq_no_2"),
    )
)

scored_employees = (
    with_windows
    .select(
        "id",
        "job_title",
        "seq_no",
        "row_seq",
        "seq_no_2",
        F.min("seq_no").over(w_ordered).alias("running_min_seq_no"),
        F.max("seq_no").over(w_ordered).alias("running_max_seq_no"),
        F.sum("seq_no").over(w_ordered).alias("running_sum_seq_no"),
        F.count("seq_no").over(w_ordered).alias("running_count_seq_no"),
    )
)

deduped = (
    scored_employees
    .withColumn("row_num", F.row_number().over(w6))
    .filter(F.col("row_num") == 1)
)

result = (
    deduped
    .select(
        "id",
        "job_title",
        "seq_no",
        "seq_no_2",
        "running_min_seq_no",
        "running_max_seq_no",
        "row_num",
    )
)
