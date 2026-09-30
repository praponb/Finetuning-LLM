from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id", "job_title")
w2 = Window.partitionBy("job_title").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("dept_id", "job_title").orderBy("salary", "id")
w4 = Window.partitionBy("job_title").orderBy("salary", "id")
w_ordered = Window.partitionBy("job_title", "dept_id").orderBy("prev_salary", "id").rowsBetween(-6, Window.currentRow)
w_hist = Window.partitionBy("dept_id").orderBy("salary", "id")
w7 = Window.orderBy("salary", "id")
salary_by_dept_id = df_employees.groupBy("dept_id").agg(F.max("salary").alias("max_employee_salary"))

base = (
    df_employees
    .filter(F.col("level").isNotNull())
    .select("*", F.substring("full_name", 1, 4).alias("full_name_prefix"))
)

metrics = (
    base
    .filter(F.month("hire_date").isin([1, 3, 5]))
    .select(
        "id",
        "full_name",
        "dept_id",
        "job_title",
        "salary",
        "is_remote",
        "level",
        F.max("salary").over(w).alias("dept_id_max_salary"),
        F.first("job_title", ignorenulls=True).over(w2).alias("first_known_job_title"),
        F.row_number().over(w3).alias("seq_no"),
    )
)

metrics_employees = (
    metrics
    .select(
        "*",
        F.lag("seq_no").over(w4).alias("prev_seq_no"),
        F.lag("salary").over(w4).alias("prev_salary"),
        F.row_number().over(w4).alias("event_seq"),
    )
)

scored = (
    metrics_employees
    .select(
        "id",
        "dept_id",
        "salary",
        "is_remote",
        "dept_id_max_salary",
        "first_known_job_title",
        "prev_seq_no",
        "prev_salary",
        F.count("prev_salary").over(w_ordered).alias("last7_count_prev_salary"),
        F.max("prev_salary").over(w_ordered).alias("last7_max_prev_salary"),
        F.min("prev_salary").over(w_ordered).alias("last7_min_prev_salary"),
    )
)

metrics_employees_2 = (
    scored
    .withColumn("next_prev_seq_no", F.lead("prev_seq_no").over(w_hist))
    .withColumn("prev_last7_min_prev_salary", F.lag("last7_min_prev_salary").over(w_hist))
    .withColumn("seq_no", F.row_number().over(w_hist))
    .withColumn("next_salary", F.lead("salary", 1).over(w7))
)

result = (
    metrics_employees_2
    .join(salary_by_dept_id, on="dept_id", how="left")
    .select(
        "id",
        "dept_id",
        "salary",
        "dept_id_max_salary",
        "prev_seq_no",
        "prev_salary",
        "last7_max_prev_salary",
        "last7_min_prev_salary",
        "next_prev_seq_no",
        F.col("max_employee_salary"),
    )
    .orderBy(F.col("last7_max_prev_salary").desc())
)
