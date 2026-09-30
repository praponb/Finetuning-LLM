from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("location").orderBy("budget", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("location")
w3 = Window.partitionBy("dept_name", "location").orderBy(F.col("budget").desc(), "id")
w4 = Window.partitionBy("location").orderBy("rn", "id")

cleaned = (
    df_departments
    .filter(F.col("dept_name") == "Finance")
    .withColumn("dept_name_lower", F.lower("dept_name"))
)

with_windows = (
    cleaned
    .filter(F.col("budget").between(1887000, 3118000))
    .withColumn("last4_avg_budget", F.avg("budget").over(w_recent))
    .withColumn("last4_count_budget", F.count("budget").over(w_recent))
    .withColumn("last4_min_budget", F.min("budget").over(w_recent))
    .withColumn("budget_share", F.round((F.col("budget") / F.sum("budget").over(w2)), 4))
)

deduped = (
    with_windows
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
)

deduped_departments = (
    deduped
    .withColumn("rank_in_group", F.row_number().over(w4))
    .filter(F.col("rank_in_group") == 1)
)

filtered = (
    deduped_departments
    .filter(F.col("last4_avg_budget") < 3426000)
    .select(
        "id",
        "dept_name",
        "budget",
        "last4_avg_budget",
        "last4_min_budget",
        "budget_share",
        "rn",
        F.coalesce("dept_name", F.lit("Unknown")).alias("dept_name_filled"),
        (F.col("budget_share") + F.col("rn")).alias("budget_share_plus_rn"),
    )
)

result = (
    filtered
    .groupBy(
        F.when(F.col("budget_share") >= 0.69, "LARGE").otherwise("SMALL").alias("budget_share_tier"),
        "dept_name",
    )
    .agg(
        F.round(F.sum("budget_share") / F.nullif(F.count(F.when(F.col("dept_name") == "Legal", 1)), F.lit(0)), 2).alias("budget_share_per_legal"),
        F.min("last4_min_budget").alias("min_last4_min_budget"),
        F.round(F.sum("budget_share") / F.nullif(F.count(F.when(F.col("dept_name") == "Sales", 1)), F.lit(0)), 2).alias("budget_share_per_sales"),
        F.countDistinct("id").alias("distinct_ids"),
        F.size(F.collect_set("dept_name")).alias("num_dept_names"),
    )
)
