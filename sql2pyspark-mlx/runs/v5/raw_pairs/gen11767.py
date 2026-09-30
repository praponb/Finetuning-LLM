from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location", "dept_name").orderBy("budget", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("location").orderBy(F.col("last4_count_budget").desc_nulls_last(), "id")
w4 = Window.partitionBy("location", "dept_name").orderBy("last4_max_budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("location").orderBy(F.col("last4_min_budget").asc_nulls_last())
w6 = Window.orderBy("last4_min_budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy("running_max_last4_avg_budget", "id").rowsBetween(-2, -1)
w8 = Window.orderBy("last4_min_budget_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.orderBy("cumulative_last4_min_budget_rank", "id")

filtered = (
    df_departments
    .filter(F.col("dept_name").isNotNull())
    .withColumn("budget_tier", F.when(F.col("budget") >= 3498000, "High").when(F.col("budget") >= 1900000, "Medium").otherwise("Low"))
)

enriched = (
    filtered
    .select(
        "id",
        "dept_name",
        "location",
        F.max("budget").over(w).alias("last4_max_budget"),
        F.min("budget").over(w).alias("last4_min_budget"),
        F.count("budget").over(w).alias("last4_count_budget"),
        F.avg("budget").over(w).alias("last4_avg_budget"),
        F.last("budget", ignorenulls=True).over(w2).alias("last_known_budget"),
    )
)

deduped = (
    enriched
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
)

metrics = (
    deduped
    .select(
        "id",
        "last4_min_budget",
        "last4_avg_budget",
        F.avg("last4_avg_budget").over(w4).alias("running_avg_last4_avg_budget"),
        F.sum("last4_avg_budget").over(w4).alias("running_sum_last4_avg_budget"),
        F.max("last4_avg_budget").over(w4).alias("running_max_last4_avg_budget"),
        F.rank().over(w5).alias("last4_min_budget_rank"),
    )
)

metrics_departments = (
    metrics
    .filter(F.col("last4_avg_budget") <= 3046000)
    .select(
        "id",
        "running_avg_last4_avg_budget",
        "running_max_last4_avg_budget",
        F.sum("last4_min_budget_rank").over(w6).alias("cumulative_last4_min_budget_rank"),
        F.avg("last4_min_budget_rank").over(w7).alias("last4_min_budget_rank_avg_2_prior"),
        F.first("last4_min_budget_rank", ignorenulls=True).over(w8).alias("first_known_last4_min_budget_rank"),
    )
)

result = (
    metrics_departments
    .select(
        "id",
        "first_known_last4_min_budget_rank",
        F.lead("last4_min_budget_rank_avg_2_prior").over(w9).alias("next_last4_min_budget_rank_avg_2_prior"),
        F.lead("cumulative_last4_min_budget_rank").over(w9).alias("next_cumulative_last4_min_budget_rank"),
        F.lag("cumulative_last4_min_budget_rank").over(w9).alias("prev_cumulative_last4_min_budget_rank"),
        F.row_number().over(w9).alias("event_seq"),
    )
)
