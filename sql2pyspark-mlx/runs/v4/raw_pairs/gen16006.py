from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy("budget", "id")

result = (
    df_departments
    .select(
        "*",
        F.ntile(10).over(w).alias("budget_bucket"),
        F.lead("budget", 1).over(w).alias("next_budget"),
    )
)
