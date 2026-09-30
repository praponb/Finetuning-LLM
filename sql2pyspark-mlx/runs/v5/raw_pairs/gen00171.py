from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy("budget", "id")
w2 = Window.partitionBy("location").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("location", "dept_name").orderBy(F.col("budget").desc())
w_part = Window.partitionBy("dept_name").orderBy("budget_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("dept_name").orderBy("budget_cume_dist", "id")
w6 = Window.partitionBy("dept_name").orderBy("budget_bucket", "id")
w7 = Window.partitionBy("dept_name").orderBy("first_known_budget_bucket", "id")
w8 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())
w9 = Window.partitionBy("dept_name")
w_ordered = Window.partitionBy("dept_name").orderBy("event_seq", "id")
w11 = Window.orderBy("budget_cume_dist", "id").rowsBetween(-5, -1)
w12 = Window.partitionBy("dept_name").orderBy("prev_budget_cume_dist", "id")
w_recent = Window.orderBy("prev_prev_budget_cume_dist", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w14 = Window.partitionBy("dept_name").orderBy(F.col("next_prev_budget_cume_dist_2").desc(), "id")

metrics = (
    df_departments
    .select(
        "id",
        "dept_name",
        F.ntile(10).over(w).alias("budget_bucket"),
        F.first("location", ignorenulls=True).over(w2).alias("first_known_location"),
        F.cume_dist().over(w3).alias("budget_cume_dist"),
    )
)

with_windows = (
    metrics
    .select(
        "id",
        "dept_name",
        "budget_cume_dist",
        F.last("budget_bucket", ignorenulls=True).over(w_part).alias("last_known_budget_bucket"),
        F.ntile(3).over(w5).alias("budget_cume_dist_bucket"),
        F.first("budget_bucket", ignorenulls=True).over(w_part).alias("first_known_budget_bucket"),
        F.lag("budget_cume_dist", 2).over(w6).alias("prev_budget_cume_dist"),
        F.last("budget_bucket", ignorenulls=True).over(w_part).alias("last_known_budget_bucket_2"),
    )
)

scored = (
    with_windows
    .select(
        "id",
        "dept_name",
        "budget_cume_dist",
        "last_known_budget_bucket",
        "budget_cume_dist_bucket",
        "first_known_budget_bucket",
        "prev_budget_cume_dist",
        F.lag("budget_cume_dist").over(w7).alias("prev_budget_cume_dist_2"),
        F.lead("prev_budget_cume_dist").over(w7).alias("next_prev_budget_cume_dist"),
        F.lag("prev_budget_cume_dist").over(w7).alias("prev_prev_budget_cume_dist"),
        F.row_number().over(w7).alias("event_seq"),
    )
)

with_latest = (
    scored
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w8))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.hire_date").alias("employee_hire_date"))
)

scored_departments = (
    with_latest
    .select(
        "id",
        "dept_name",
        "budget_cume_dist",
        "budget_cume_dist_bucket",
        "first_known_budget_bucket",
        "prev_budget_cume_dist",
        "prev_budget_cume_dist_2",
        "next_prev_budget_cume_dist",
        "prev_prev_budget_cume_dist",
        "event_seq",
        "employee_hire_date",
        F.max("budget_cume_dist").over(w9).alias("dept_name_max_budget_cume_dist"),
        F.lead("prev_budget_cume_dist_2", 1).over(w_ordered).alias("next_prev_budget_cume_dist_2"),
        F.avg("first_known_budget_bucket").over(w11).alias("first_known_budget_bucket_avg_5_prior"),
        F.ntile(4).over(w12).alias("prev_budget_cume_dist_bucket"),
    )
)

result = (
    scored_departments
    .select(
        "id",
        "dept_name",
        "budget_cume_dist_bucket",
        "first_known_budget_bucket",
        "prev_budget_cume_dist",
        "prev_budget_cume_dist_2",
        "next_prev_budget_cume_dist",
        "event_seq",
        "first_known_budget_bucket_avg_5_prior",
        F.round((F.col("prev_budget_cume_dist_bucket") / F.sum("prev_budget_cume_dist_bucket").over(w9)), 4).alias("prev_budget_cume_dist_bucket_share"),
        F.sum("next_prev_budget_cume_dist").over(w_recent).alias("cumulative_next_prev_budget_cume_dist"),
        F.row_number().over(w14).alias("event_seq_2"),
    )
)
