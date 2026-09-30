from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name", "location").orderBy("budget", "id").rowsBetween(-3, -1)
w2 = Window.partitionBy("location", "dept_name").orderBy("budget", "id")

valid = (
    df_departments
    .filter(F.col("location").isNotNull() & (F.col("dept_name") != "HR"))
    .select(
        "*",
        F.when(F.col("location").isin(["Tokyo", "Bangkok"]), "Priority").otherwise("Other").alias("location_group"),
        F.when(F.col("location").isin(["Singapore", "Bangkok"]), "Priority").otherwise("Other").alias("location_group_2"),
    )
)

result = (
    valid
    .select(
        "id",
        "location",
        F.avg("budget").over(w).alias("budget_avg_3_prior"),
        F.lag("budget", 1, 0).over(w2).alias("prev_budget"),
    )
)
