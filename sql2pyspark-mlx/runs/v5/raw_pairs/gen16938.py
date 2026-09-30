from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy("budget_filled", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.orderBy("budget", "id")
w3 = Window.orderBy(F.col("last7_count_budget").desc(), "id")

cleaned = (
    df_departments
    .filter(F.col("location") == "London")
    .withColumn("budget_filled", F.coalesce("budget", F.lit(0)))
    .withColumn("clean_location", F.trim("location"))
)

metrics = (
    cleaned
    .select(
        "*",
        F.sum("budget").over(w).alias("last7_sum_budget"),
        F.avg("budget").over(w).alias("last7_avg_budget"),
        F.count("budget").over(w).alias("last7_count_budget"),
    )
)

kept = (
    metrics
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (~F.col("x.level").isin(["L1", "L5"])), "left_anti")
)

enriched = (
    kept
    .filter(F.col("last7_count_budget") == 12)
    .select(
        "id",
        "budget",
        "budget_filled",
        "last7_sum_budget",
        "last7_count_budget",
        F.lead("last7_sum_budget", 1, 0).over(w2).alias("next_last7_sum_budget"),
        F.ntile(3).over(w3).alias("last7_count_budget_bucket"),
    )
)

result = (
    enriched
    .select(
        "id",
        "last7_sum_budget",
        "next_last7_sum_budget",
        "last7_count_budget_bucket",
    )
    .orderBy(F.col("last7_sum_budget").desc_nulls_first())
)
