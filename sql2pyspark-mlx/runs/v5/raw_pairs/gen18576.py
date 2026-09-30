from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy(F.col("budget").desc(), "id")
w2 = Window.partitionBy("location").orderBy("budget_adjusted", "id")
w3 = Window.partitionBy("dept_name").orderBy(F.col("budget_rounded").desc(), "id")
w4 = Window.orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("dept_name", "location")
w_ordered = Window.partitionBy("location", "dept_name").orderBy("budget_bucket", "id").rowsBetween(-6, Window.currentRow)

cleaned = (
    df_departments
    .filter(F.col("budget") < 1271000)
    .withColumn("budget_rounded", F.round("budget", 0))
    .withColumn("budget_adjusted", F.round((F.col("budget") * 1.07), 2))
)

valid = (
    cleaned
    .filter(F.col("budget_rounded").between(56, 67))
    .select(
        "id",
        "dept_name",
        "location",
        "budget",
        "budget_rounded",
        "budget_adjusted",
        F.trim("location").alias("clean_location"),
        F.concat_ws("/", "dept_name", "location").alias("dept_name_location_key"),
    )
)

metrics = (
    valid
    .select(
        "id",
        "dept_name",
        "location",
        "budget",
        "budget_adjusted",
        "clean_location",
        "dept_name_location_key",
        F.ntile(10).over(w).alias("budget_bucket"),
        F.ntile(10).over(w2).alias("budget_adjusted_bucket"),
        F.ntile(3).over(w3).alias("budget_rounded_bucket"),
        F.sum("budget_adjusted").over(w4).alias("running_budget_adjusted"),
        F.max("budget").over(w5).alias("dept_name_max_budget"),
    )
)

enriched = (
    metrics
    .withColumn("last7_sum_budget", F.sum("budget").over(w_ordered))
    .withColumn("last7_avg_budget", F.avg("budget").over(w_ordered))
    .withColumn("last7_max_budget", F.max("budget").over(w_ordered))
    .withColumn("last7_min_budget", F.min("budget").over(w_ordered))
)

result = (
    enriched
    .groupBy("dept_name", "location")
    .agg(
        F.stddev_samp("budget").alias("budget_stddev"),
        F.count(F.when(F.col("location") == "Berlin", 1)).alias("berlin_count"),
        F.size(F.collect_set("location")).alias("num_locations"),
    )
)
