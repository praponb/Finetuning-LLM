from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy(F.col("budget").desc())
w2 = Window.partitionBy("location")

result = (
    df_departments
    .filter(F.col("budget") > 1150000)
    .select(
        "id",
        "dept_name",
        "budget",
        "location",
        F.percent_rank().over(w).alias("budget_pct_rank"),
        F.avg("budget").over(w2).alias("location_avg_budget"),
    )
)
