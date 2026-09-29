from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name", "location").orderBy(F.col("budget").asc_nulls_last(), "id")
w2 = Window.partitionBy("location")
w3 = Window.partitionBy("dept_name")
w4 = Window.orderBy(F.col("adjusted_budget").asc_nulls_last())
w5 = Window.orderBy(F.col("budget_whole").desc(), "id")
w6 = Window.partitionBy("location", "dept_name").orderBy("budget")
w7 = Window.partitionBy("dept_name").orderBy("budget_whole_bucket", "id")

filtered = (
    df_departments
    .filter((~F.col("dept_name").eqNullSafe(F.lit("HR"))) & F.col("dept_name").isNotNull())
    .select(
        "*",
        F.round((F.col("budget") * 0.9), 2).alias("adjusted_budget"),
        F.coalesce("dept_name", F.lit("UNKNOWN")).alias("resolved_dept_name"),
    )
)

base = (
    filtered
    .filter((~F.col("location").eqNullSafe(F.lit("Berlin"))) & ~F.col("location").isin(["Singapore", "Bangkok", "London"]))
    .select(
        "id",
        "dept_name",
        "location",
        "budget",
        "adjusted_budget",
        F.ceil("budget").alias("budget_whole"),
        F.coalesce("location", F.lit("N/A")).alias("location_filled"),
    )
)

ranked = (
    base
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

valid = (
    ranked
    .filter(F.col("budget") < 3568000)
    .select(
        "*",
        F.when(F.col("dept_name").isin(["Sales", "Legal"]), "Group A").otherwise("Other").alias("dept_name_group"),
        F.coalesce("location", F.lit("DIRECT")).alias("location_filled_2"),
    )
)

enriched = (
    valid
    .withColumn("location_avg_adjusted_budget", F.avg("adjusted_budget").over(w2))
    .withColumn("dept_name_min_budget_whole", F.min("budget_whole").over(w3))
    .withColumn("adjusted_budget_percent_rank", F.percent_rank().over(w4))
    .withColumn("budget_whole_bucket", F.ntile(4).over(w5))
    .withColumn("budget_dense_rank", F.dense_rank().over(w6))
)

eligible = (
    enriched
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (~F.col("x.is_remote")), "left_anti")
)

metrics = (
    eligible
    .withColumn("prev_location_avg_adjusted_budget", F.lag("location_avg_adjusted_budget").over(w7))
    .withColumn("prev_budget_whole", F.lag("budget_whole").over(w7))
    .withColumn("next_budget_whole", F.lead("budget_whole").over(w7))
    .withColumn("seq_no", F.row_number().over(w7))
)

result = (
    metrics
    .groupBy(
        "location",
        F.when(F.col("next_budget_whole") >= 73, "HIGH").otherwise("LOW").alias("next_budget_whole_tier"),
    )
    .agg(
        F.round(F.avg("next_budget_whole"), 2).alias("avg_next_budget_whole"),
        F.max(F.when(F.col("dept_name") == "Engineering", F.col("adjusted_budget"))).alias("engineering_max_adjusted_budget"),
        F.avg("budget_whole").alias("avg_budget_whole"),
        F.round(F.sum("next_budget_whole") / F.nullif(F.count(F.when(F.col("dept_name") == "HR", 1)), F.lit(0)), 2).alias("next_budget_whole_per_hr"),
        F.sum(F.when(F.col("dept_name") == "Sales", F.col("budget")).otherwise(0)).alias("sales_budget"),
    )
    .orderBy("next_budget_whole_per_hr")
)
