from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(-6, Window.currentRow)

result = (
    df_departments
    .filter(F.col("location") == "Chiang Mai")
    .select(
        "id",
        "dept_name",
        "location",
        F.count("budget").over(w_recent).alias("last7_count_budget"),
        F.max("budget").over(w_recent).alias("last7_max_budget"),
        F.sum("budget").over(w_recent).alias("last7_sum_budget"),
        F.min("budget").over(w_recent).alias("last7_min_budget"),
    )
)
