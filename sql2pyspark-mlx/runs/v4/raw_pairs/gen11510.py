from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy("budget", "id")
w2 = Window.orderBy("budget", "id")

result = (
    df_departments
    .select(
        "*",
        F.ntile(4).over(w).alias("budget_bucket"),
        F.lag("budget", 1).over(w2).alias("prev_budget"),
    )
)
