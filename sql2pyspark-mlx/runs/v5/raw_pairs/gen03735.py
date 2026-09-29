from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("manager_id").orderBy("budget", "id")
w2 = Window.partitionBy("job_title", "location").orderBy("hire_date", "id").rowsBetween(-6, Window.currentRow)
w3 = Window.partitionBy("location").orderBy(F.col("prev_budget").desc())
w_running = Window.partitionBy("location").orderBy("salary_avg_6", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("salary_avg_6", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("job_title").orderBy("prev_budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist_2 = Window.partitionBy("level").orderBy("salary_avg_6", "id")

joined = (
    df_employees
    .alias("e")
    .join(df_departments.alias("p"), F.col("e.dept_id") == F.col("p.id"), "left")
    .select("e.*", F.col("p.location"), F.col("p.budget"))
)

scored = (
    joined
    .select(
        "id",
        "dept_id",
        "manager_id",
        "job_title",
        "level",
        "location",
        F.lag("budget", 2).over(w_part).alias("prev_budget"),
        F.avg("salary").over(w2).alias("salary_avg_6"),
    )
)

ranked = (
    scored
    .withColumn("dedup_rank", F.rank().over(w3))
    .filter(F.col("dedup_rank") == 1)
)

metrics = (
    ranked
    .select(
        "id",
        "dept_id",
        "level",
        "location",
        "prev_budget",
        "dedup_rank",
        F.first("level", ignorenulls=True).over(w_running).alias("first_known_level"),
        F.sum("prev_budget").over(w5).alias("running_prev_budget"),
        F.sum("salary_avg_6").over(w_hist).alias("running_salary_avg_6"),
        F.lag("salary_avg_6", 1).over(w_hist_2).alias("prev_salary_avg_6"),
    )
)

with_dims = (
    metrics
    .alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "left")
    .select(
        "e.id",
        "e.dept_id",
        "e.location",
        "e.dedup_rank",
        "e.first_known_level",
        "e.running_prev_budget",
        "e.running_salary_avg_6",
        F.col("d.budget"),
    )
)

result = (
    with_dims
    .alias("s")
    .join(df_departments.alias("p"), F.col("s.dept_id") == F.col("p.id"), "left")
    .rollup(
        F.col("p.location").alias("department_location"),
        F.col("p.dept_name").alias("dept_name"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.max(F.col("s.running_prev_budget")).alias("max_running_prev_budget"),
        F.grouping_id().alias("grouping_level"),
    )
)
