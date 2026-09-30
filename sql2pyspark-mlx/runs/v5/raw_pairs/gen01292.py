from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("dept_name").orderBy("budget", "id")
w2 = Window.partitionBy("location").orderBy("budget", "id")
w3 = Window.partitionBy("location").orderBy("next_budget", "id")
w4 = Window.partitionBy("location").orderBy("next_budget", "id").rowsBetween(-6, -1)
w5 = Window.partitionBy("location").orderBy(F.col("prev_budget").desc(), "id")
w6 = Window.partitionBy("location").orderBy(F.col("next_budget_2_avg_6_prior").desc_nulls_first(), "id")
w_hist = Window.partitionBy("location").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("location")
w9 = Window.partitionBy("location").orderBy(F.col("prev_budget_bucket").desc())
w10 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())

scored = (
    df_departments
    .select(
        "id",
        "location",
        "budget",
        F.lag("budget").over(w_grp).alias("prev_budget"),
        F.lead("budget").over(w_grp).alias("next_budget"),
        F.row_number().over(w_grp).alias("seq_no"),
        F.lead("budget", 1).over(w2).alias("next_budget_2"),
    )
)

with_windows = (
    scored
    .select(
        "id",
        "location",
        "budget",
        "next_budget_2",
        F.lag("prev_budget", 1, 0).over(w3).alias("prev_prev_budget"),
        F.avg("next_budget_2").over(w4).alias("next_budget_2_avg_6_prior"),
        F.ntile(10).over(w5).alias("prev_budget_bucket"),
    )
)

deduped = (
    with_windows
    .withColumn("latest_rank", F.row_number().over(w6))
    .filter(F.col("latest_rank") == 1)
    .select("id", "location", "budget", "prev_prev_budget", "prev_budget_bucket")
)

enriched = (
    deduped
    .withColumn("running_sum_prev_budget_bucket", F.sum("prev_budget_bucket").over(w_hist))
    .withColumn("running_max_prev_budget_bucket", F.max("prev_budget_bucket").over(w_hist))
    .withColumn("running_min_prev_budget_bucket", F.min("prev_budget_bucket").over(w_hist))
    .withColumn("prev_prev_budget_share", F.round((F.col("prev_prev_budget") / F.sum("prev_prev_budget").over(w8)), 4))
)

ranked = (
    enriched
    .withColumn("rank_in_group", F.rank().over(w9))
    .filter(F.col("rank_in_group") == 1)
)

latest_match = (
    ranked
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w10))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.location",
        "s.budget",
        "s.prev_prev_budget",
        "s.prev_budget_bucket",
        "s.running_sum_prev_budget_bucket",
        "s.running_max_prev_budget_bucket",
        "s.prev_prev_budget_share",
        F.col("c.job_title").alias("employee_job_title"),
        F.col("c.level").alias("employee_level"),
    )
)

eligible = (
    latest_match
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.col("c.email").like("%@startup.io")), "left_anti")
)

result = (
    eligible
    .select(
        "id",
        "prev_budget_bucket",
        "running_sum_prev_budget_bucket",
        "running_max_prev_budget_bucket",
        "employee_job_title",
        F.when(F.col("employee_job_title").rlike("^(Manager|Designer)$"), "CORE").when(F.col("employee_job_title").rlike("^(Engineer|Analyst)$"), "OTHER_ZONE").otherwise("PRIORITY").alias("employee_job_title_zone"),
    )
    .orderBy(F.col("employee_job_title_zone").desc())
)
