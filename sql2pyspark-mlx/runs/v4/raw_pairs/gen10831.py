from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy(F.col("budget").desc(), "id")
w2 = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("location")
w_grp = Window.partitionBy("dept_name").orderBy("budget")
w5 = Window.partitionBy("location").orderBy("budget_percent_rank", "id").rowsBetween(-3, -1)
w6 = Window.partitionBy("location").orderBy("event_seq", "id")
w_hist = Window.partitionBy("dept_name").orderBy(F.col("budget").asc_nulls_last())
w8 = Window.partitionBy("dept_name").orderBy(F.col("last_known_budget").desc(), "id")
w9 = Window.partitionBy("dept_name").orderBy(F.col("event_seq").desc(), "id")
w10 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())
w11 = Window.partitionBy("dept_name")
w12 = Window.partitionBy("location", "dept_name").orderBy(F.col("last_known_budget").desc())
w13 = Window.partitionBy("dept_name").orderBy(F.col("tokyo_max_budget").desc())

enriched = (
    df_departments
    .select(
        "*",
        F.row_number().over(w).alias("event_seq"),
        F.last("budget", ignorenulls=True).over(w2).alias("last_known_budget"),
        F.sum("budget").over(w_running).alias("location_sum_budget"),
        F.percent_rank().over(w_grp).alias("budget_percent_rank"),
    )
)

metrics = (
    enriched
    .withColumn("location_sum_budget_avg_3_prior", F.avg("location_sum_budget").over(w5))
    .withColumn("event_seq_bucket", F.ntile(4).over(w6))
    .withColumn("budget_dense_rank", F.dense_rank().over(w_hist))
    .withColumn("last_known_budget_bucket", F.ntile(10).over(w8))
    .withColumn("event_seq_2", F.row_number().over(w9))
)

kept = (
    metrics
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.level") == "L3"), "left_anti")
)

with_latest = (
    kept
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w10))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.hire_date"))
)

scored = (
    with_latest
    .withColumn("dept_name_avg_budget_percent_rank", F.avg("budget_percent_rank").over(w11))
    .withColumn("last_known_budget_rank", F.rank().over(w12))
    .withColumn("location_avg_budget", F.avg("budget").over(w_running))
)

daily = (
    scored
    .groupBy("dept_name", F.date_trunc("MONTH", "hire_date").alias("activity_month"))
    .agg(
        F.avg(F.when(F.col("location") == "Chiang Mai", F.col("event_seq_2"))).alias("chiang_mai_avg_event_seq_2"),
        F.percentile_approx("last_known_budget", [0.25, 0.5, 0.75]).alias("last_known_budget_quartiles"),
        F.round(F.sum("event_seq_bucket") / F.nullif(F.count(F.when(F.col("dept_name") == "Finance", 1)), F.lit(0)), 2).alias("event_seq_bucket_per_finance"),
        F.max(F.when(F.col("location") == "Tokyo", F.col("budget"))).alias("tokyo_max_budget"),
    )
    .withColumn("tokyo_max_budget_rank", F.rank().over(w13))
)

result = (
    daily
    .select(
        "dept_name",
        "activity_month",
        "chiang_mai_avg_event_seq_2",
        "last_known_budget_quartiles",
        "event_seq_bucket_per_finance",
        "tokyo_max_budget",
        "tokyo_max_budget_rank",
    )
    .orderBy(F.col("tokyo_max_budget").desc_nulls_last())
)
