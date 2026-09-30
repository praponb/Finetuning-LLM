from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("budget_int_tier").orderBy(F.col("singapore_events").desc())

cleaned = (
    df_departments
    .filter(F.col("dept_name") == "Legal")
    .select(
        "*",
        F.col("budget").cast("int").alias("budget_int"),
        F.trim("dept_name").alias("clean_dept_name"),
    )
)

with_latest = (
    cleaned
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.hire_date").alias("employee_hire_date"))
)

latest_match = (
    with_latest
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.col("c.hire_date") <= F.col("s.employee_hire_date")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.location",
        "s.budget",
        "s.budget_int",
        F.col("c.salary").alias("employee_salary"),
    )
)

not_flagged = (
    latest_match
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.job_title") == "Engineer"), "left_anti")
)

result = (
    not_flagged
    .groupBy(
        F.when(F.col("budget_int") >= 87, "PREMIUM").otherwise("STANDARD").alias("budget_int_tier"),
        "location",
    )
    .agg(
        F.count_if(F.col("location") == "London").alias("london_events"),
        F.count_if(F.col("location") == "Singapore").alias("singapore_events"),
        F.count("*").alias("row_count"),
    )
    .withColumn("singapore_events_dense_rank", F.dense_rank().over(w2))
    .orderBy(F.col("row_count").desc())
)
