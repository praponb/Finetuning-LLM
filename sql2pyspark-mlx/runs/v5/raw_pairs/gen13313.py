from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_departments
    .select(
        "id",
        "location",
        "budget",
        F.max("budget").over(w).alias("running_max_budget"),
        F.count("budget").over(w).alias("running_count_budget"),
        F.min("budget").over(w).alias("running_min_budget"),
        F.sum("budget").over(w).alias("running_sum_budget"),
    )
)
