from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy("budget_whole", "id").rowsBetween(-5, Window.currentRow)
w_running = Window.partitionBy("dept_name")
w_hist = Window.partitionBy("dept_name", "location").orderBy("budget_whole", "id")
w_grp = Window.partitionBy("location", "dept_name").orderBy("budget_whole", "id").rowsBetween(-5, Window.currentRow)
w5 = Window.partitionBy("location", "dept_name").orderBy(F.col("dept_name_max_budget_whole").desc(), "id")
w_part = Window.partitionBy("dept_name", "location").orderBy("budget_whole", "id").rowsBetween(-2, -1)
w7 = Window.partitionBy("location").orderBy(F.col("prev_budget").desc_nulls_last())
w_recent = Window.partitionBy("location").orderBy("dept_name_max_budget_whole", "id")
w9 = Window.partitionBy("dept_name", "location").orderBy("latest_rank", "id")

valid = (
    df_departments
    .filter(F.col("location").isNotNull())
    .select("*", F.floor("budget").alias("budget_whole"))
)

enriched = (
    valid
    .select(
        "*",
        F.avg("budget_whole").over(w).alias("budget_whole_avg_5"),
        F.max("budget_whole").over(w_running).alias("dept_name_max_budget_whole"),
        F.lag("budget", 1).over(w_hist).alias("prev_budget"),
        F.avg("budget_whole").over(w_grp).alias("budget_whole_avg_5_2"),
    )
)

deduped = (
    enriched
    .withColumn("latest_rank", F.row_number().over(w5))
    .filter(F.col("latest_rank") == 1)
)

metrics = (
    deduped
    .select(
        "id",
        "dept_name",
        "budget",
        "budget_whole",
        "prev_budget",
        F.avg("budget_whole").over(w_part).alias("budget_whole_avg_2_prior"),
        F.dense_rank().over(w7).alias("prev_budget_dense_rank"),
        F.lead("budget", 1).over(w_recent).alias("next_budget"),
        F.lead("budget_whole_avg_5", 1).over(w9).alias("next_budget_whole_avg_5"),
    )
)

result = (
    metrics
    .select(
        "id",
        "dept_name",
        "budget",
        "prev_budget",
        "budget_whole_avg_2_prior",
        "next_budget",
        "next_budget_whole_avg_5",
    )
)
