from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy(F.col("budget").asc_nulls_last())
w_running = Window.orderBy("budget", "id")
w3 = Window.partitionBy("location").orderBy(F.col("budget").desc(), "id")
w_running_2 = Window.orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("dept_name").orderBy("budget", "id")

with_windows = (
    df_departments
    .select(
        "id",
        "dept_name",
        F.rank().over(w).alias("budget_rank"),
        F.lag("budget", 2).over(w_running).alias("prev_budget"),
        F.row_number().over(w3).alias("event_seq"),
        F.sum("budget").over(w_running_2).alias("running_budget"),
        F.row_number().over(w_grp).alias("seq_no"),
    )
)

result = (
    with_windows
    .withColumn("prev_budget_tier", F.when(F.col("prev_budget") >= 3203000, "HIGH").otherwise("LOW"))
    .cube("seq_no", "prev_budget_tier")
    .agg(
        F.max("prev_budget").alias("max_prev_budget"),
        F.percentile_approx("seq_no", 0.9).alias("seq_no_p90"),
        F.size(F.collect_set("dept_name")).alias("num_dept_names"),
        F.grouping("seq_no").alias("grouping_id"),
    )
    .filter(F.col("seq_no_p90") >= 0)
)
