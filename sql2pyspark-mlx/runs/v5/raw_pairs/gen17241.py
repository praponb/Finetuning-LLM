from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("location").orderBy(F.col("budget").desc(), "id")
w_grp = Window.partitionBy("location").orderBy("budget", "id").rowsBetween(-2, Window.currentRow)
w3 = Window.partitionBy("dept_name").orderBy("budget", "id")
w4 = Window.partitionBy("dept_name").orderBy(F.col("budget_avg_2").asc_nulls_last())
w5 = Window.orderBy(F.col("budget").desc_nulls_first())
w6 = Window.partitionBy("dept_name").orderBy("seq_no", "id").rowsBetween(-6, Window.currentRow)
w_running = Window.partitionBy("dept_name").orderBy("budget_percent_rank", "id")
w_hist = Window.partitionBy("dept_name").orderBy(F.col("budget_avg_2_percent_rank").desc(), "id")

metrics = (
    df_departments
    .select(
        "id",
        "dept_name",
        "budget",
        F.row_number().over(w_recent).alias("seq_no"),
        F.avg("budget").over(w_grp).alias("budget_avg_2"),
        F.row_number().over(w3).alias("seq_no_2"),
    )
)

scored = (
    metrics
    .select(
        "id",
        "dept_name",
        "budget",
        "seq_no",
        F.percent_rank().over(w4).alias("budget_avg_2_percent_rank"),
        F.percent_rank().over(w5).alias("budget_percent_rank"),
        F.avg("seq_no_2").over(w6).alias("seq_no_2_avg_6"),
    )
)

metrics_departments = (
    scored
    .filter(F.col("budget_avg_2_percent_rank") > 0.36)
    .select(
        "id",
        "dept_name",
        "seq_no",
        "budget_percent_rank",
        "seq_no_2_avg_6",
        F.lead("seq_no").over(w_running).alias("next_seq_no"),
        F.lag("seq_no").over(w_running).alias("prev_seq_no"),
        F.lead("seq_no_2_avg_6").over(w_running).alias("next_seq_no_2_avg_6"),
        F.lag("seq_no_2_avg_6").over(w_running).alias("prev_seq_no_2_avg_6"),
        F.row_number().over(w_running).alias("event_seq"),
        F.ntile(3).over(w_hist).alias("budget_avg_2_percent_rank_bucket"),
    )
)

kept_keys = (
    metrics_departments
    .select("id")
    .intersect(
        df_departments
        .filter(F.col("dept_name") == "HR")
        .select("id")
    )
)

verified = (
    metrics_departments
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

remaining_keys = (
    verified
    .select("dept_name")
    .intersect(
        df_departments
        .filter(F.col("budget").between(2427000, 2836000))
        .select("dept_name")
    )
)

reconciled = (
    verified
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.dept_name") == F.col("k.dept_name")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy("prev_seq_no")
    .agg(
        F.percentile_approx("next_seq_no", [0.25, 0.5, 0.75]).alias("next_seq_no_quartiles"),
        F.count(F.when(F.col("dept_name") == "Sales", 1)).alias("sales_count"),
    )
    .filter(F.col("sales_count") >= 1)
)
