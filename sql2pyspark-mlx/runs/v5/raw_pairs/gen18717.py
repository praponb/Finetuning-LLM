from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("dept_name").orderBy("running_avg_budget", "id")
w4 = Window.partitionBy("dept_name").orderBy("running_sum_budget", "id").rowsBetween(-5, Window.currentRow)

with_latest = (
    df_departments
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.level"))
)

filtered = (
    with_latest
    .filter((~F.col("level").eqNullSafe(F.lit("L4"))) & (F.col("budget") >= 2613000))
    .select("id", "dept_name", "budget", F.upper("dept_name").alias("dept_name_upper"))
)

with_windows = (
    filtered
    .select(
        "id",
        "dept_name",
        F.max("budget").over(w2).alias("running_max_budget"),
        F.count("budget").over(w2).alias("running_count_budget"),
        F.avg("budget").over(w2).alias("running_avg_budget"),
        F.sum("budget").over(w2).alias("running_sum_budget"),
    )
)

metrics = (
    with_windows
    .select(
        "*",
        F.lag("running_max_budget").over(w3).alias("prev_running_max_budget"),
        F.lead("running_avg_budget").over(w3).alias("next_running_avg_budget"),
        F.lag("running_avg_budget").over(w3).alias("prev_running_avg_budget"),
        F.row_number().over(w3).alias("row_seq"),
        F.avg("running_sum_budget").over(w4).alias("running_sum_budget_avg_5"),
    )
)

eligible = (
    metrics
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.month(F.col("x.hire_date")).isin([9, 11, 12])), "left_semi")
)

result = (
    eligible
    .withColumn("prev_running_max_budget_tier", F.when(F.col("prev_running_max_budget") >= 3496000, "HIGH").otherwise("LOW"))
    .rollup("prev_running_max_budget_tier")
    .agg(
        F.sum(F.when(F.col("dept_name") == "Sales", F.col("row_seq")).otherwise(0)).alias("sales_row_seq"),
        F.round(F.sum("running_sum_budget_avg_5") / F.nullif(F.count(F.when(F.col("dept_name") == "Finance", 1)), F.lit(0)), 2).alias("running_sum_budget_avg_5_per_finance"),
        F.count(F.when(F.col("dept_name") == "Marketing", 1)).alias("marketing_rows"),
        F.countDistinct("dept_name").alias("distinct_dept_names"),
        F.round(F.sum("running_max_budget") / F.nullif(F.count(F.when(F.col("dept_name") == "HR", 1)), F.lit(0)), 2).alias("running_max_budget_per_hr"),
        F.sum(F.when(F.col("dept_name") == "Engineering", F.col("running_count_budget")).otherwise(0)).alias("engineering_running_count_budget"),
        F.grouping("prev_running_max_budget_tier").alias("grouping_id"),
    )
    .orderBy(F.col("sales_row_seq").desc())
)
