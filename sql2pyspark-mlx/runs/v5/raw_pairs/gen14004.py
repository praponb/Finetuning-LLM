from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy("location_length", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(-5, Window.currentRow)
w3 = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("dept_name").orderBy("location_length", "id")
w5 = Window.partitionBy("dept_name").orderBy(F.col("running_max_budget").desc(), "id")
w6 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())

cleaned = (
    df_departments
    .filter(F.col("dept_name").isNotNull())
    .select(
        "*",
        F.length("location").alias("location_length"),
        F.coalesce("dept_name", F.lit("DIRECT")).alias("dept_name_filled"),
    )
)

metrics = (
    cleaned
    .filter(F.col("budget") < 3463000)
    .select(
        "id",
        "dept_name",
        "budget",
        "location_length",
        F.max("budget").over(w).alias("running_max_budget"),
        F.min("budget").over(w).alias("running_min_budget"),
        F.sum("budget").over(w).alias("running_sum_budget"),
        F.avg("budget").over(w2).alias("budget_avg_5"),
    )
)

metrics_departments = (
    metrics
    .select(
        "id",
        "dept_name",
        "location_length",
        "running_max_budget",
        "running_sum_budget",
        "budget_avg_5",
        F.sum("budget_avg_5").over(w3).alias("running_budget_avg_5"),
        F.row_number().over(w4).alias("event_seq"),
        F.row_number().over(w5).alias("seq_no"),
    )
)

not_flagged = (
    metrics_departments
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.dept_id").isin([15, 25, 28])), "left_semi")
)

with_last = (
    not_flagged
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.dept_name",
        "s.running_sum_budget",
        "s.running_budget_avg_5",
        "s.event_seq",
        "s.seq_no",
        F.col("c.job_title"),
        F.col("c.salary").alias("employee_salary"),
    )
)

result = (
    with_last
    .select(
        "id",
        "dept_name",
        "running_sum_budget",
        "running_budget_avg_5",
        "event_seq",
        "seq_no",
        "job_title",
        F.when(F.col("dept_name").rlike("^(Engineering|Sales)$"), "OTHER_ZONE").otherwise("CORE").alias("dept_name_zone"),
    )
    .orderBy(F.col("seq_no").desc(), "running_sum_budget")
)
