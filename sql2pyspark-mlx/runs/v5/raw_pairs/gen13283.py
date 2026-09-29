from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("location").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_departments
    .select(
        "id",
        "location",
        "budget",
        F.min("budget").over(w_ordered).alias("running_min_budget"),
        F.sum("budget").over(w_ordered).alias("running_sum_budget"),
        F.count("budget").over(w_ordered).alias("running_count_budget"),
        F.last("budget", ignorenulls=True).over(w2).alias("last_known_budget"),
    )
)
