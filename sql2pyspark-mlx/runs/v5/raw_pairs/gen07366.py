from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location", "dept_name")
w2 = Window.partitionBy("dept_name", "location").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("location")

metrics = (
    df_departments
    .filter(F.col("dept_name") == "Finance")
    .select(
        "*",
        F.round((F.col("budget") / F.sum("budget").over(w)), 4).alias("budget_share"),
        F.last("dept_name", ignorenulls=True).over(w2).alias("last_known_dept_name"),
        F.min("budget").over(w3).alias("location_min_budget"),
    )
)

result = (
    metrics
    .groupBy("dept_name")
    .agg(
        F.sum(F.when(F.col("location") == "Berlin", F.col("budget_share")).otherwise(0)).alias("berlin_budget_share"),
        F.sum(F.when(F.col("location") == "Bangkok", F.col("budget")).otherwise(0)).alias("bangkok_budget"),
        F.size(F.collect_set("location")).alias("num_locations"),
        F.sum(F.when(F.col("dept_name") == "Engineering", F.col("budget")).otherwise(0)).alias("engineering_budget"),
    )
)
