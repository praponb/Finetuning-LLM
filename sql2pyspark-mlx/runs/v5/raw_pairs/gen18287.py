from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("level", "manager_id").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("dept_id").orderBy("salary", "id").rowsBetween(-6, Window.currentRow)
w3 = Window.orderBy(F.col("salary").desc())
w4 = Window.orderBy(F.col("hire_date").desc(), "id")
w5 = Window.orderBy("running_count_salary", "id")
w6 = Window.partitionBy("job_title", "level")
w7 = Window.partitionBy("manager_id").orderBy("salary_dense_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("dept_id").orderBy("event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("level").orderBy("running_sum_salary", "id")

filtered = (
    df_employees
    .filter((F.col("hire_date") > F.lit("2023-06-07").cast("date")) & (F.col("job_title").eqNullSafe(F.lit("Intern"))))
    .withColumn("email_title", F.initcap(F.trim("email")))
    .withColumn("level_filled", F.coalesce("level", F.lit("NONE")))
)

with_windows = (
    filtered
    .filter(F.col("level").isin(["L5", "L4"]))
    .select(
        "*",
        F.sum("salary").over(w_part).alias("running_sum_salary"),
        F.avg("salary").over(w_part).alias("running_avg_salary"),
        F.count("salary").over(w_part).alias("running_count_salary"),
        F.avg("salary").over(w2).alias("salary_avg_6"),
    )
)

with_windows_employees = (
    with_windows
    .withColumn("salary_dense_rank", F.dense_rank().over(w3))
    .withColumn("event_seq", F.row_number().over(w4))
    .withColumn("next_salary", F.lead("salary", 2).over(w5))
)

scored = (
    with_windows_employees
    .select(
        "id",
        "full_name",
        "dept_id",
        "manager_id",
        "job_title",
        "hire_date",
        "is_remote",
        "email_title",
        "salary_dense_rank",
        F.sum("next_salary").over(w6).alias("job_title_sum_next_salary"),
        F.sum("next_salary").over(w7).alias("cumulative_next_salary"),
        F.sum("salary_dense_rank").over(w8).alias("cumulative_salary_dense_rank"),
        F.row_number().over(w9).alias("event_seq_2"),
    )
)

result = (
    scored
    .groupBy("manager_id", "dept_id")
    .pivot("job_title", ["Designer", "Manager"])
    .agg(F.max("salary_dense_rank"))
)
