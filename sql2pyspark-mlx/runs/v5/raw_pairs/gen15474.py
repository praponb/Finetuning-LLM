from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("employee_job_title").orderBy(F.col("employee_salary").desc(), "id")
w4 = Window.partitionBy("employee_job_title").orderBy("salary")
w_hist = Window.partitionBy("employee_job_title").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("employee_job_title").orderBy("employee_salary", "id")
w7 = Window.partitionBy("employee_job_title").orderBy("employee_salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("employee_job_title").orderBy(F.col("salary").desc(), "id")
w9 = Window.orderBy("salary", "id")
w10 = Window.orderBy("cumulative_salary", "id").rowsBetween(-2, Window.currentRow)
w11 = Window.orderBy("event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w12 = Window.orderBy("event_seq", "id")

base = (
    df_departments
    .filter(F.col("location").isNotNull())
    .withColumn("is_marketing", F.when(F.col("dept_name") == "Marketing", 1).otherwise(0))
    .withColumn("dept_name_filled", F.coalesce("dept_name", F.lit("OTHER")))
)

latest_match = (
    base
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.dept_name_filled", F.col("c.salary"))
)

latest_match_departments = (
    latest_match
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.job_title").alias("employee_job_title"),
        F.col("c.salary").alias("employee_salary"),
    )
)

latest = (
    latest_match_departments
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

metrics = (
    latest
    .select(
        "id",
        "salary",
        "employee_salary",
        F.cume_dist().over(w4).alias("salary_cume_dist"),
        F.first("employee_job_title", ignorenulls=True).over(w_hist).alias("first_known_employee_job_title"),
        F.ntile(4).over(w6).alias("employee_salary_bucket"),
        F.sum("salary").over(w7).alias("cumulative_salary"),
        F.row_number().over(w8).alias("event_seq"),
    )
)

scored = (
    metrics
    .withColumn("seq_no", F.row_number().over(w9))
    .withColumn("salary_avg_2", F.avg("salary").over(w10))
    .withColumn("last_known_cumulative_salary", F.last("cumulative_salary", ignorenulls=True).over(w11))
    .withColumn("prev_salary_cume_dist", F.lag("salary_cume_dist", 2).over(w12))
    .withColumn("prev_salary", F.lag("salary", 2).over(w9))
)

result = (
    scored
    .select(
        "id",
        "salary",
        "employee_salary_bucket",
        "cumulative_salary",
        "event_seq",
        "salary_avg_2",
        "last_known_cumulative_salary",
        "prev_salary_cume_dist",
        "prev_salary",
    )
    .orderBy(F.col("salary_avg_2").desc())
)
