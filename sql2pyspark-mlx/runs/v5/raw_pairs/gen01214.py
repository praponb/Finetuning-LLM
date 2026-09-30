from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("dept_name", "location").orderBy("budget", "id").rowsBetween(-3, Window.currentRow)
w_grp_2 = Window.partitionBy("dept_name", "location").orderBy("budget", "id")

with_windows = (
    df_departments
    .select(
        "id",
        "budget",
        F.avg("budget").over(w_grp).alias("last4_avg_budget"),
        F.count("budget").over(w_grp).alias("last4_count_budget"),
        F.max("budget").over(w_grp).alias("last4_max_budget"),
        F.lead("budget", 2).over(w_grp_2).alias("next_budget"),
    )
)

result = (
    with_windows
    .select("id", "budget", "last4_avg_budget", "last4_max_budget", "next_budget")
    .orderBy("budget")
)
