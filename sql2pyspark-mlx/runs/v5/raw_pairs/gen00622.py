from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy("budget", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.orderBy("budget", "id")
w_recent = Window.partitionBy("location", "dept_name").orderBy("prev_budget", "id").rowsBetween(-2, Window.currentRow)
w4 = Window.partitionBy("location").orderBy("last3_avg_last3_max_budget", "id")
w5 = Window.orderBy("last3_avg_last3_max_budget", "id").rowsBetween(-5, -1)

filtered = (
    df_departments
    .filter(F.col("dept_name").isNotNull())
    .withColumn("dept_name_location_key", F.concat_ws("/", "dept_name", "location"))
    .withColumn("dept_name_filled", F.coalesce("dept_name", F.lit("OTHER")))
)

metrics = (
    filtered
    .select(
        "id",
        "dept_name",
        "location",
        "dept_name_location_key",
        "dept_name_filled",
        F.max("budget").over(w).alias("last3_max_budget"),
        F.avg("budget").over(w).alias("last3_avg_budget"),
        F.count("budget").over(w).alias("last3_count_budget"),
        F.lag("budget", 2).over(w2).alias("prev_budget"),
    )
)

not_flagged = (
    metrics
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.col("c.manager_id").isin([6, 18, 29])), "left_anti")
)

enriched = (
    not_flagged
    .select(
        "*",
        F.count("last3_max_budget").over(w_recent).alias("last3_count_last3_max_budget"),
        F.avg("last3_max_budget").over(w_recent).alias("last3_avg_last3_max_budget"),
        F.max("last3_max_budget").over(w_recent).alias("last3_max_last3_max_budget"),
    )
)

kept = (
    enriched
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.lower(F.trim(F.col("ch.full_name"))) == "ivy chen"), "left_anti")
)

result = (
    kept
    .filter(F.col("last3_max_last3_max_budget") > 2690000)
    .withColumn("last3_avg_last3_max_budget_bucket", F.ntile(4).over(w4))
    .withColumn("last3_avg_last3_max_budget_avg_5_prior", F.avg("last3_avg_last3_max_budget").over(w5))
)
