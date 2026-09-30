from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("dept_name", "location").orderBy(F.col("budget").desc())

result = (
    df_departments
    .filter(~F.col("location").isin(["New York", "Bangkok"]))
    .select(
        "id",
        "location",
        "budget",
        F.min("budget").over(w).alias("running_min_budget"),
        F.count("budget").over(w).alias("running_count_budget"),
        F.avg("budget").over(w).alias("running_avg_budget"),
        F.dense_rank().over(w2).alias("budget_dense_rank"),
    )
)
