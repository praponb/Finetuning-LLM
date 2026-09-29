from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy("budget", "id")
w2 = Window.partitionBy("location")
w3 = Window.orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("dept_name").orderBy("budget")

with_windows = (
    df_departments
    .filter(F.col("location").isin(["Berlin", "Bangkok"]))
    .select(
        "id",
        "location",
        "budget",
        F.lead("budget", 1).over(w).alias("next_budget"),
        F.max("budget").over(w2).alias("location_max_budget"),
        F.sum("budget").over(w3).alias("running_budget"),
        F.cume_dist().over(w4).alias("budget_cume_dist"),
    )
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("location_max_budget") >= 3139113, "LARGE").otherwise("SMALL").alias("location_max_budget_tier"),
    )
    .agg(
        F.count_if(F.col("location") == "Bangkok").alias("bangkok_events"),
        F.percentile_approx("running_budget", 0.95).alias("running_budget_p95"),
        F.count(F.when(F.col("location") == "Tokyo", 1)).alias("tokyo_rows"),
    )
    .orderBy(F.col("location_max_budget_tier").desc())
)
