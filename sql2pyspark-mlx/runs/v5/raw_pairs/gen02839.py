from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("location").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_departments
    .filter(F.col("location") == "Chiang Mai")
    .select(
        "id",
        "location",
        F.max("budget").over(w_running).alias("running_max_budget"),
        F.avg("budget").over(w_running).alias("running_avg_budget"),
        F.min("budget").over(w_running).alias("running_min_budget"),
    )
)
