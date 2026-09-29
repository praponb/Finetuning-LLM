from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy("hire_date", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("job_title")
w3 = Window.partitionBy("job_title").orderBy(F.col("salary_share").desc(), "id")
w4 = Window.partitionBy("dept_id").orderBy("hire_date", "id").rowsBetween(-3, Window.currentRow)
w5 = Window.partitionBy("manager_id").orderBy("last4_count_salary", "id")
w6 = Window.orderBy(F.col("last4_count_salary").desc(), "id")
w_ordered = Window.partitionBy("manager_id").orderBy("last4_avg_salary", "id").rowsBetween(-3, Window.currentRow)
w_running = Window.partitionBy("manager_id")
w9 = Window.orderBy("manager_id_count_last4_max_salary", "id")
w10 = Window.orderBy(F.col("last4_count_salary_bucket").asc_nulls_first())
w11 = Window.orderBy(F.col("manager_id_count_last4_max_salary").desc(), "id")
w12 = Window.orderBy(F.col("prev_last4_sum_salary").desc(), "id")
w13 = Window.orderBy("last4_count_salary_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

metrics = (
    df_employees
    .filter(F.col("manager_id") == 6)
    .select(
        "*",
        F.avg("salary").over(w).alias("last4_avg_salary"),
        F.sum("salary").over(w).alias("last4_sum_salary"),
        F.min("salary").over(w).alias("last4_min_salary"),
        F.round((F.col("salary") / F.sum("salary").over(w2)), 4).alias("salary_share"),
    )
)

deduped = (
    metrics
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
    .select("id", "dept_id", "manager_id", "salary", "hire_date")
)

metrics_employees = (
    deduped
    .select(
        "id",
        "manager_id",
        "salary",
        F.avg("salary").over(w4).alias("last4_avg_salary"),
        F.sum("salary").over(w4).alias("last4_sum_salary"),
        F.count("salary").over(w4).alias("last4_count_salary"),
        F.max("salary").over(w4).alias("last4_max_salary"),
    )
)

with_windows = (
    metrics_employees
    .select(
        "*",
        F.lag("last4_count_salary").over(w5).alias("prev_last4_count_salary"),
        F.lead("last4_sum_salary").over(w5).alias("next_last4_sum_salary"),
        F.lag("last4_sum_salary").over(w5).alias("prev_last4_sum_salary"),
        F.row_number().over(w5).alias("seq_no"),
        F.ntile(3).over(w6).alias("last4_count_salary_bucket"),
    )
)

metrics_employees_2 = (
    with_windows
    .select(
        "id",
        "salary",
        "last4_sum_salary",
        "prev_last4_count_salary",
        "prev_last4_sum_salary",
        "seq_no",
        "last4_count_salary_bucket",
        F.sum("seq_no").over(w_ordered).alias("last4_sum_seq_no"),
        F.min("seq_no").over(w_ordered).alias("last4_min_seq_no"),
        F.max("seq_no").over(w_ordered).alias("last4_max_seq_no"),
        F.count("last4_max_salary").over(w_running).alias("manager_id_count_last4_max_salary"),
    )
)

cleaned = (
    metrics_employees_2
    .filter(F.col("last4_sum_seq_no") <= 35)
    .withColumn("last4_min_seq_no_level", F.when(F.col("last4_min_seq_no") >= 8.0, "High").when(F.col("last4_min_seq_no") >= 2.9, "Medium").otherwise("Low"))
    .withColumn("prev_last4_sum_salary_plus_manager_id_count_last4_max_salary", (F.col("prev_last4_sum_salary") + F.col("manager_id_count_last4_max_salary")))
)

scored = (
    cleaned
    .select(
        "id",
        "prev_last4_count_salary",
        "seq_no",
        "last4_sum_seq_no",
        "prev_last4_sum_salary_plus_manager_id_count_last4_max_salary",
        F.lag("last4_max_seq_no", 1).over(w9).alias("prev_last4_max_seq_no"),
        F.dense_rank().over(w10).alias("last4_count_salary_bucket_dense_rank"),
        F.row_number().over(w11).alias("seq_no_2"),
        F.ntile(5).over(w12).alias("prev_last4_sum_salary_bucket"),
        F.sum("last4_sum_salary").over(w13).alias("running_last4_sum_salary"),
    )
)

result = (
    scored
    .select(
        "id",
        "prev_last4_count_salary",
        "seq_no",
        "last4_sum_seq_no",
        "prev_last4_sum_salary_plus_manager_id_count_last4_max_salary",
        "prev_last4_max_seq_no",
        "seq_no_2",
        "running_last4_sum_salary",
    )
    .orderBy(F.col("last4_sum_seq_no").desc())
)
