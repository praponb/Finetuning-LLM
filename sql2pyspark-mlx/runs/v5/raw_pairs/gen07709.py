from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("dept_name")
w2 = Window.partitionBy("location", "dept_name").orderBy(F.col("budget").desc(), "id")
w_recent = Window.partitionBy("location").orderBy("budget")
w4 = Window.partitionBy("dept_name", "location").orderBy("budget", "id")
w5 = Window.partitionBy("location", "dept_name").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_windows = (
    df_departments
    .withColumn("dept_name_max_budget", F.max("budget").over(w_grp))
    .withColumn("seq_no", F.row_number().over(w2))
    .withColumn("budget_dense_rank", F.dense_rank().over(w_recent))
    .withColumn("budget_bucket", F.ntile(10).over(w4))
    .withColumn("running_budget", F.sum("budget").over(w5))
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("budget_bucket") >= 7, "HIGH").otherwise("LOW").alias("budget_bucket_tier"),
        F.when(F.col("budget_dense_rank") >= 8, "LARGE").otherwise("SMALL").alias("budget_dense_rank_tier"),
        F.when(F.col("running_budget") >= 19088276, "HIGH").otherwise("LOW").alias("running_budget_tier"),
    )
    .agg(
        F.count(F.when(F.col("location") == "Singapore", 1)).alias("singapore_count"),
        F.min("dept_name_max_budget").alias("min_dept_name_max_budget"),
        F.percentile_approx("seq_no", [0.25, 0.5, 0.75]).alias("seq_no_quartiles"),
        F.sum(F.when(F.col("dept_name") == "HR", F.col("budget_bucket")).otherwise(0)).alias("hr_budget_bucket"),
    )
    .orderBy(F.col("hr_budget_bucket").desc())
)
