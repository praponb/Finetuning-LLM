from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy(F.col("budget").desc(), "id")
w_running = Window.partitionBy("location", "dept_name").orderBy(F.col("budget").asc_nulls_first())
w_part = Window.partitionBy("location").orderBy(F.col("budget").desc(), "id")

scored = (
    df_departments
    .select(
        "*",
        F.ntile(5).over(w).alias("budget_bucket"),
        F.percent_rank().over(w_running).alias("budget_percent_rank"),
        F.ntile(4).over(w_part).alias("budget_bucket_2"),
    )
)

result = (
    scored
    .groupBy(
        "location",
        F.when(F.col("budget_bucket_2") >= 7, "HIGH").otherwise("LOW").alias("budget_bucket_2_tier"),
    )
    .agg(
        F.count(F.when(F.col("dept_name") == "HR", 1)).alias("hr_count"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.avg("budget_percent_rank"), 2).alias("avg_budget_percent_rank"),
        F.percentile_approx("budget_percent_rank", 0.5).alias("budget_percent_rank_p50"),
        F.countDistinct("dept_name").alias("distinct_dept_names"),
        F.countDistinct("id").alias("distinct_ids_2"),
    )
    .filter(F.col("distinct_ids_2") > 1)
    .orderBy(F.col("budget_bucket_2_tier").desc_nulls_last())
)
