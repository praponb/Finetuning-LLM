from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("location")
w3 = Window.partitionBy("location").orderBy("last7_count_is_legal", "id")
w4 = Window.orderBy("is_legal", "id")
w5 = Window.partitionBy("dept_name").orderBy(F.col("last7_min_is_legal").desc(), "id")
w_hist = Window.partitionBy("dept_name").orderBy(F.col("budget_share").desc(), "id")
w_hist_2 = Window.orderBy(F.col("budget_share").desc(), "id")
w8 = Window.orderBy("next_last7_min_is_legal", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_departments
    .filter(F.col("location") != "London")
    .select(
        "*",
        F.when(F.col("dept_name") == "Legal", 1).otherwise(0).alias("is_legal"),
        F.upper("location").alias("location_upper"),
        F.coalesce("location", F.lit("N/A")).alias("resolved_location"),
    )
)

metrics = (
    cleaned
    .withColumn("last7_min_is_legal", F.min("is_legal").over(w_grp))
    .withColumn("last7_sum_is_legal", F.sum("is_legal").over(w_grp))
    .withColumn("last7_count_is_legal", F.count("is_legal").over(w_grp))
    .withColumn("budget_share", F.round((F.col("budget") / F.sum("budget").over(w2)), 4))
)

metrics_departments = (
    metrics
    .filter(F.col("is_legal") > 80)
    .select(
        "*",
        F.lag("budget", 2).over(w3).alias("prev_budget"),
        F.lead("last7_min_is_legal", 1).over(w4).alias("next_last7_min_is_legal"),
        F.ntile(3).over(w5).alias("last7_min_is_legal_bucket"),
    )
)

filtered = (
    metrics_departments
    .filter(~F.col("location").eqNullSafe(F.lit("London")))
    .select(
        "*",
        F.col("is_legal").cast("double").alias("is_legal_str"),
        F.round("prev_budget", 2).alias("prev_budget_rounded"),
        F.coalesce("location", F.lit("DIRECT")).alias("location_clean"),
    )
)

with_windows = (
    filtered
    .withColumn("prev_budget_share", F.round((F.col("prev_budget") / F.sum("prev_budget").over(w2)), 4))
    .withColumn("budget_share_bucket", F.ntile(4).over(w_hist))
    .withColumn("budget_share_bucket_2", F.ntile(3).over(w_hist_2))
    .withColumn("last_known_next_last7_min_is_legal", F.last("next_last7_min_is_legal", ignorenulls=True).over(w8))
)

result = (
    with_windows
    .select(
        "id",
        "dept_name",
        "location",
        "budget",
        "location_upper",
        "last7_min_is_legal",
        "last7_sum_is_legal",
        "budget_share",
        "prev_budget",
    )
    .orderBy("last7_sum_is_legal")
)
