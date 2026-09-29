from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("location", "dept_name").orderBy(F.col("budget").desc())
w_grp = Window.partitionBy("dept_name").orderBy("budget", "id")
w_ordered = Window.orderBy("budget_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy(F.col("budget_rank").desc(), "id")
w5 = Window.orderBy("budget_rank_rounded", "id")
w6 = Window.orderBy("running_budget_rank_rounded", "id")

with_windows = (
    df_departments
    .select(
        "id",
        "dept_name",
        F.rank().over(w_hist).alias("budget_rank"),
        F.ntile(5).over(w_grp).alias("budget_bucket"),
    )
)

valid = (
    with_windows
    .filter(~F.col("dept_name").eqNullSafe(F.lit("Marketing")))
    .withColumn("budget_bucket_num", F.col("budget_bucket").cast("string"))
    .withColumn("budget_rank_rounded", F.round("budget_rank", 2))
)

cleaned = (
    valid
    .filter(F.col("dept_name").isNotNull() & (~F.col("dept_name").eqNullSafe(F.lit("HR"))))
    .select(
        "id",
        "budget_rank",
        "budget_bucket_num",
        "budget_rank_rounded",
        F.when(F.col("budget_rank") >= 7, "High").when(F.col("budget_rank") >= 4, "Medium").otherwise("Low").alias("budget_rank_band"),
        F.initcap(F.trim("dept_name")).alias("dept_name_title"),
    )
)

enriched = (
    cleaned
    .select(
        "*",
        F.sum("budget_rank_rounded").over(w_ordered).alias("running_budget_rank_rounded"),
        F.ntile(5).over(w4).alias("budget_rank_bucket"),
    )
)

metrics = (
    enriched
    .select(
        "*",
        F.lag("running_budget_rank_rounded").over(w5).alias("prev_running_budget_rank_rounded"),
        F.lead("running_budget_rank_rounded").over(w5).alias("next_running_budget_rank_rounded"),
        F.lag("budget_rank_rounded").over(w5).alias("prev_budget_rank_rounded"),
        F.lead("budget_rank_rounded").over(w5).alias("next_budget_rank_rounded"),
        F.row_number().over(w5).alias("row_seq"),
        F.lead("budget_rank_bucket", 1, 0).over(w6).alias("next_budget_rank_bucket"),
    )
)

result = (
    metrics
    .select(
        "id",
        "budget_rank",
        "budget_rank_rounded",
        "budget_rank_band",
        "next_running_budget_rank_rounded",
        "next_budget_rank_rounded",
        "next_budget_rank_bucket",
    )
)
