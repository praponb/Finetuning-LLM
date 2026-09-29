from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("location", "dept_name").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("location")
w4 = Window.partitionBy("dept_name")
w5 = Window.partitionBy("location").orderBy(F.col("running_budget_share").desc_nulls_last())
w_recent = Window.orderBy(F.col("running_budget_share_cume_dist").desc())
w7 = Window.partitionBy("location").orderBy(F.col("running_budget_share_filled").desc())

scored = (
    df_departments
    .select(
        "id",
        "dept_name",
        "location",
        F.last("location", ignorenulls=True).over(w_hist).alias("last_known_location"),
        F.sum("budget").over(w2).alias("running_budget"),
    )
)

kept_keys = (
    scored
    .select("dept_name")
    .intersect(
        df_departments
        .filter(F.col("location") != "Chiang Mai")
        .select("dept_name")
    )
)

kept_rows = (
    scored
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.dept_name") == F.col("k.dept_name")), "inner")
    .select("s.*")
)

metrics = (
    kept_rows
    .filter(F.col("dept_name").isNotNull())
    .select(
        "location",
        "last_known_location",
        F.round((F.col("running_budget") / F.sum("running_budget").over(w3)), 4).alias("running_budget_share"),
        F.count("running_budget").over(w4).alias("dept_name_count_running_budget"),
    )
)

enriched = (
    metrics
    .select(
        "location",
        "last_known_location",
        "running_budget_share",
        F.cume_dist().over(w5).alias("running_budget_share_cume_dist"),
        F.sum("dept_name_count_running_budget").over(w3).alias("location_sum_dept_name_count_running_budget"),
    )
)

metrics_departments = (
    enriched
    .withColumn("location_count_running_budget_share", F.count("running_budget_share").over(w3))
    .withColumn("running_budget_share_cume_dist_rank", F.rank().over(w_recent))
)

base = (
    metrics_departments
    .filter((F.col("location_sum_dept_name_count_running_budget") != 44) & F.col("location").isNotNull())
    .select(
        "*",
        F.col("location_sum_dept_name_count_running_budget").cast("double").alias("location_sum_dept_name_count_running_budget_num"),
        F.coalesce("running_budget_share", F.lit(0)).alias("running_budget_share_filled"),
        F.coalesce("location", F.lit("DIRECT")).alias("location_filled"),
    )
)

enriched_departments = (
    base
    .select(
        "last_known_location",
        "running_budget_share",
        "running_budget_share_cume_dist",
        "location_count_running_budget_share",
        "running_budget_share_filled",
        "location_filled",
        F.dense_rank().over(w7).alias("running_budget_share_filled_dense_rank"),
    )
)

result = (
    enriched_departments
    .select(
        "last_known_location",
        "running_budget_share",
        "running_budget_share_cume_dist",
        "running_budget_share_filled",
        "location_filled",
    )
    .orderBy("running_budget_share_filled")
)
