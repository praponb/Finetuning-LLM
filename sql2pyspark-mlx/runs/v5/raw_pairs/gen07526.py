from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy(F.col("hire_date").desc_nulls_last())
w_running = Window.partitionBy("job_title").orderBy("hire_date", "id")
w_ordered = Window.partitionBy("manager_id")
w4 = Window.partitionBy("dept_id", "level").orderBy("manager_id_stddev_samp_salary", "id")
w5 = Window.partitionBy("dept_id")
w6 = Window.partitionBy("dept_name", "location").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy(F.col("hire_date").desc(), "id")
w8 = Window.partitionBy("dept_id").orderBy("manager_id_stddev_samp_salary", "id")
w_recent = Window.partitionBy("manager_id").orderBy("running_max_seq_no", "id")
w_hist = Window.partitionBy("dept_id", "dept_name")
w_hist_2 = Window.partitionBy("manager_id", "dept_name").orderBy("running_min_seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

latest = (
    df_employees
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
)

enriched = (
    latest
    .withColumn("prev_salary", F.lag("salary").over(w_running))
    .withColumn("next_salary", F.lead("salary").over(w_running))
    .withColumn("seq_no", F.row_number().over(w_running))
    .withColumn("manager_id_stddev_samp_salary", F.stddev_samp("salary").over(w_ordered))
)

enriched_employees = (
    enriched
    .select(
        "id",
        "dept_id",
        "manager_id",
        "hire_date",
        "is_remote",
        "seq_no",
        "manager_id_stddev_samp_salary",
        F.lag("prev_salary").over(w4).alias("prev_prev_salary"),
        F.lag("salary").over(w4).alias("prev_salary_2"),
        F.row_number().over(w4).alias("row_seq"),
        F.round((F.col("seq_no") / F.sum("seq_no").over(w5)), 4).alias("seq_no_share"),
    )
)

enriched_employees_2 = (
    enriched_employees
    .alias("a")
    .join(df_departments.alias("d"), F.col("a.dept_id") == F.col("d.id"), "left")
    .select(
        "a.id",
        "a.dept_id",
        "a.manager_id",
        "a.hire_date",
        "a.seq_no",
        "a.manager_id_stddev_samp_salary",
        "a.prev_salary_2",
        F.col("d.budget"),
        F.col("d.location"),
        F.col("d.dept_name"),
    )
)

with_windows = (
    enriched_employees_2
    .filter(F.col("manager_id_stddev_samp_salary") != 105247)
    .select(
        "id",
        "dept_id",
        "manager_id",
        "hire_date",
        "seq_no",
        "manager_id_stddev_samp_salary",
        "budget",
        "dept_name",
        F.max("seq_no").over(w6).alias("running_max_seq_no"),
        F.sum("seq_no").over(w6).alias("running_sum_seq_no"),
        F.min("seq_no").over(w6).alias("running_min_seq_no"),
    )
)

result = (
    with_windows
    .select(
        "*",
        F.row_number().over(w7).alias("seq_no_2"),
        F.lag("running_sum_seq_no", 2, 0).over(w8).alias("prev_running_sum_seq_no"),
        F.row_number().over(w_recent).alias("seq_no_3"),
        F.max("running_min_seq_no").over(w_hist).alias("dept_id_max_running_min_seq_no"),
        F.sum("running_sum_seq_no").over(w_hist_2).alias("running_running_sum_seq_no"),
    )
)
