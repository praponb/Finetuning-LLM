from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy(F.col("budget").desc(), "id")
w2 = Window.partitionBy("location", "dept_name").orderBy(F.col("budget").desc_nulls_first(), "id")
w_running = Window.partitionBy("location", "dept_name").orderBy("budget", "id")
w4 = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("location", "dept_name").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

ranked = (
    df_departments
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

ranked_departments = (
    ranked
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

enriched = (
    ranked_departments
    .select(
        "*",
        F.ntile(4).over(w_running).alias("budget_bucket"),
        F.first("dept_name", ignorenulls=True).over(w4).alias("first_known_dept_name"),
    )
)

metrics = (
    enriched
    .select(
        "*",
        F.sum("budget").over(w4).alias("running_budget"),
        F.last("budget", ignorenulls=True).over(w5).alias("last_known_budget"),
    )
)

result = (
    metrics
    .groupBy(
        F.when(F.col("last_known_budget") >= 3068000, "PREMIUM").otherwise("STANDARD").alias("last_known_budget_tier"),
        F.when(F.col("budget_bucket") >= 7, "LARGE").otherwise("SMALL").alias("budget_bucket_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("location")).alias("location_list"),
        F.stddev_samp("budget").alias("budget_stddev"),
        F.min("budget_bucket").alias("min_budget_bucket"),
        F.size(F.collect_set("location")).alias("num_locations"),
        F.percentile_approx("running_budget", [0.25, 0.5, 0.75]).alias("running_budget_quartiles"),
    )
    .orderBy(F.col("budget_stddev").desc_nulls_first())
)
