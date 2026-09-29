from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("location").orderBy(F.col("budget").asc_nulls_first())
w_running = Window.partitionBy("location").orderBy("budget", "id").rowsBetween(-2, -1)
w4 = Window.partitionBy("location").orderBy(F.col("budget").desc(), "id")
w5 = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(-6, Window.currentRow)
w6 = Window.partitionBy("location").orderBy("budget_avg_2_prior", "id")
w7 = Window.partitionBy("location")
w8 = Window.partitionBy("dept_name").orderBy("budget_dense_rank", "id")
w_grp = Window.partitionBy("location").orderBy("seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.orderBy("budget", "id").rowsBetween(-3, -1)

with_latest = (
    df_departments
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.hire_date").alias("employee_hire_date"))
)

metrics = (
    with_latest
    .select(
        "*",
        F.dense_rank().over(w2).alias("budget_dense_rank"),
        F.avg("budget").over(w_running).alias("budget_avg_2_prior"),
    )
)

kept = (
    metrics
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.dept_id") == 15), "left_anti")
)

metrics_departments = (
    kept
    .select(
        "*",
        F.row_number().over(w4).alias("event_seq"),
        F.avg("budget_avg_2_prior").over(w5).alias("budget_avg_2_prior_avg_6"),
        F.row_number().over(w6).alias("seq_no"),
        F.round((F.col("budget_dense_rank") / F.sum("budget_dense_rank").over(w7)), 4).alias("budget_dense_rank_share"),
    )
)

result = (
    metrics_departments
    .select(
        "*",
        F.ntile(3).over(w8).alias("budget_dense_rank_bucket"),
        F.last("dept_name", ignorenulls=True).over(w_grp).alias("last_known_dept_name"),
        F.avg("seq_no").over(w_recent).alias("seq_no_avg_3_prior"),
    )
)
