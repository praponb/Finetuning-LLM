from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name")
w2 = Window.partitionBy("dept_name", "location").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_departments
    .filter(F.col("budget").between(1789000, 2967000))
    .withColumn("location_dept_name_key", F.concat_ws("-", "location", "dept_name"))
    .withColumn("location_filled", F.coalesce("location", F.lit("DIRECT")))
)

metrics = (
    filtered
    .select(
        "*",
        F.max("budget").over(w).alias("dept_name_max_budget"),
        F.sum("budget").over(w2).alias("running_budget"),
    )
)

base = (
    metrics
    .filter(F.col("dept_name") == "Legal")
    .select(
        "*",
        F.round("budget", 1).alias("budget_rounded"),
        F.when(F.col("budget") >= 3995000, "High").when(F.col("budget") >= 763000, "Medium").otherwise("Low").alias("budget_level"),
    )
)

result = (
    base
    .withColumn("budget_rounded_tier", F.when(F.col("budget_rounded") >= 87, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["dept_name", "budget_rounded_tier"], ["dept_name"]],
        "dept_name",
        "budget_rounded_tier",
    )
    .agg(
        F.size(F.collect_set("location")).alias("num_locations"),
        F.round(F.avg("running_budget"), 2).alias("avg_running_budget"),
        F.avg("budget").alias("avg_budget"),
        F.round(F.avg("budget"), 2).alias("avg_budget_2"),
    )
    .filter(F.col("avg_running_budget") >= 0)
)
