from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("location", "dept_name").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

metrics = (
    df_departments
    .select(
        "id",
        "dept_name",
        "location",
        "budget",
        F.avg("budget").over(w_ordered).alias("running_avg_budget"),
        F.min("budget").over(w_ordered).alias("running_min_budget"),
        F.count("budget").over(w_ordered).alias("running_count_budget"),
    )
)

result = (
    metrics
    .groupBy(
        F.when(F.col("running_min_budget") >= 4118000, "PREMIUM").otherwise("STANDARD").alias("running_min_budget_tier"),
        F.when(F.col("budget") >= 3227000, "HIGH").otherwise("LOW").alias("budget_tier"),
    )
    .agg(
        F.count(F.when(F.col("dept_name") == "Legal", 1)).alias("legal_count"),
        F.round(F.sum("running_avg_budget") / F.nullif(F.count(F.when(F.col("location") == "Chiang Mai", 1)), F.lit(0)), 2).alias("running_avg_budget_per_chiang_mai"),
        F.count("*").alias("cnt"),
        F.max(F.when(F.col("dept_name") == "Finance", F.col("budget"))).alias("finance_max_budget"),
        F.avg(F.when(F.col("location") == "Tokyo", F.col("budget"))).alias("tokyo_avg_budget"),
    )
    .orderBy(F.col("finance_max_budget").asc_nulls_last())
)
