from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("dept_name", "location").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("dept_name", "location").orderBy("budget", "id")

result = (
    df_departments
    .select(
        "id",
        "location",
        "budget",
        F.max("budget").over(w_ordered).alias("running_max_budget"),
        F.count("budget").over(w_ordered).alias("running_count_budget"),
        F.min("budget").over(w_ordered).alias("running_min_budget"),
        F.lead("budget", 2, 0).over(w2).alias("next_budget"),
    )
)
