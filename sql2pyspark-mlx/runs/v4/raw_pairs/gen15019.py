from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy(F.col("budget").desc_nulls_first())
w2 = Window.partitionBy("location")
w_running = Window.partitionBy("location", "dept_name").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("dept_name").orderBy("budget", "id")
w5 = Window.orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_departments
    .select(
        "*",
        F.percent_rank().over(w).alias("budget_percent_rank"),
        F.count("budget").over(w2).alias("location_count_budget"),
        F.sum("budget").over(w_running).alias("cumulative_budget"),
        F.row_number().over(w4).alias("seq_no"),
        F.last("budget", ignorenulls=True).over(w5).alias("last_known_budget"),
    )
)

result = (
    enriched
    .select("id", "dept_name", "location_count_budget", "last_known_budget")
    .orderBy(F.col("dept_name").desc())
)
