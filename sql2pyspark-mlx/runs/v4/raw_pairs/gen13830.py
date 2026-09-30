from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy("dept_name_length", "id")
w_part = Window.orderBy("budget", "id").rowsBetween(-3, -1)
w3 = Window.orderBy("budget", "id")

filtered = (
    df_departments
    .filter(F.col("dept_name").isNotNull())
    .withColumn("dept_name_length", F.length("dept_name"))
)

result = (
    filtered
    .select(
        "*",
        F.lead("budget", 2).over(w).alias("next_budget"),
        F.avg("budget").over(w_part).alias("budget_avg_3_prior"),
        F.lag("budget", 2).over(w3).alias("prev_budget"),
    )
)
