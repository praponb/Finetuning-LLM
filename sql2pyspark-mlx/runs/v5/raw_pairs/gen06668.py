from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("dept_name").orderBy("budget", "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("location").orderBy("prev_budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.orderBy(F.col("prev_budget").desc(), "id")
w5 = Window.partitionBy("location").orderBy("next_budget", "id")
w6 = Window.partitionBy("location").orderBy("next_budget_bucket", "id")
w_recent = Window.partitionBy("location")

metrics = (
    df_departments
    .select(
        "id",
        "dept_name",
        "location",
        F.lag("budget").over(w_hist).alias("prev_budget"),
        F.lead("budget").over(w_hist).alias("next_budget"),
        F.row_number().over(w_hist).alias("row_seq"),
    )
)

eligible = (
    metrics
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.email").like("%@gmail.com")), "left_anti")
)

with_latest = (
    eligible
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.salary"), F.col("c.level"))
)

scored = (
    with_latest
    .select(
        "id",
        "location",
        "prev_budget",
        "next_budget",
        "row_seq",
        F.first("salary", ignorenulls=True).over(w3).alias("first_known_salary"),
        F.row_number().over(w_part).alias("seq_no"),
        F.ntile(5).over(w5).alias("next_budget_bucket"),
    )
)

metrics_departments = (
    scored
    .filter(F.col("first_known_salary") < 112000)
    .withColumn("prev_prev_budget", F.lag("prev_budget").over(w6))
    .withColumn("next_prev_budget", F.lead("prev_budget").over(w6))
    .withColumn("next_first_known_salary", F.lead("first_known_salary").over(w6))
    .withColumn("event_seq", F.row_number().over(w6))
    .withColumn("location_sum_prev_budget", F.sum("prev_budget").over(w_recent))
)

result = (
    metrics_departments
    .select(
        "id",
        "prev_budget",
        "next_budget",
        "first_known_salary",
        "seq_no",
        "prev_prev_budget",
        "next_prev_budget",
        "event_seq",
        "location_sum_prev_budget",
    )
    .orderBy("id", "event_seq")
)
