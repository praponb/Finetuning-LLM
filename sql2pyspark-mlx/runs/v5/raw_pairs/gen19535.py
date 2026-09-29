from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy(F.col("budget_rounded").desc_nulls_first())
w_running = Window.partitionBy("dept_name", "location").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy(F.col("budget_rounded").desc(), "id")

cleaned = (
    df_departments
    .filter(F.col("location").isNotNull())
    .select("*", F.round("budget", 1).alias("budget_rounded"))
)

result = (
    cleaned
    .filter(F.col("location").isin(["Singapore", "Chiang Mai", "New York"]))
    .select(
        "id",
        "location",
        "budget",
        "budget_rounded",
        F.percent_rank().over(w).alias("budget_rounded_percent_rank"),
        F.sum("budget_rounded").over(w_running).alias("cumulative_budget_rounded"),
        F.ntile(5).over(w3).alias("budget_rounded_bucket"),
    )
)
