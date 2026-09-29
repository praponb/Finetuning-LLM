from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("budget").desc_nulls_first())
w2 = Window.orderBy("budget", "id").rowsBetween(-3, Window.currentRow)
w_grp = Window.partitionBy("dept_name", "location").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("location").orderBy("budget", "id")

scored = (
    df_departments
    .select(
        "*",
        F.rank().over(w).alias("budget_rank"),
        F.avg("budget").over(w2).alias("budget_avg_3"),
        F.sum("budget").over(w_grp).alias("cumulative_budget"),
        F.ntile(10).over(w4).alias("budget_bucket"),
    )
)

result = scored.select("id", "location", "budget_rank", "budget_avg_3", "budget_bucket")
