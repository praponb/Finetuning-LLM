from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("location").orderBy(F.col("budget").desc(), "id")
w4 = Window.partitionBy("dept_name").orderBy(F.col("first_known_budget").desc_nulls_first(), "id")
w5 = Window.partitionBy("location", "dept_name").orderBy("first_known_budget", "id")
w6 = Window.partitionBy("dept_name").orderBy(F.col("budget_bucket").desc(), "id")
w7 = Window.partitionBy("dept_name").orderBy("last_known_budget_filled", "id").rowsBetween(-6, -1)

cleaned = (
    df_departments
    .filter(F.col("location").isNotNull())
    .withColumn("location_filled", F.coalesce("location", F.lit("Unknown")))
    .withColumn("budget_filled", F.coalesce("budget", F.lit(0)))
    .withColumn("location_clean", F.coalesce("location", F.lit("DIRECT")))
)

with_windows = (
    cleaned
    .withColumn("last_known_budget_filled", F.last("budget_filled", ignorenulls=True).over(w))
    .withColumn("first_known_budget", F.first("budget", ignorenulls=True).over(w2))
    .withColumn("budget_bucket", F.ntile(4).over(w_grp))
)

deduped = (
    with_windows
    .withColumn("latest_rank", F.row_number().over(w4))
    .filter(F.col("latest_rank") == 1)
)

with_windows_departments = (
    deduped
    .withColumn("prev_first_known_budget", F.lag("first_known_budget", 1).over(w5))
    .withColumn("seq_no", F.row_number().over(w6))
    .withColumn("budget_avg_6_prior", F.avg("budget").over(w7))
)

result = (
    with_windows_departments
    .select(
        "id",
        "location",
        "budget",
        "location_filled",
        "budget_filled",
        "location_clean",
        "last_known_budget_filled",
        "budget_bucket",
        "prev_first_known_budget",
    )
    .orderBy("budget_bucket", F.col("last_known_budget_filled").desc())
)
