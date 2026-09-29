from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("budget").desc(), "id")

result = (
    df_departments
    .filter(~F.col("dept_name").isin(["Finance", "Marketing"]))
    .select(
        "id",
        "dept_name",
        F.count("budget").over(w_recent).alias("running_count_budget"),
        F.min("budget").over(w_recent).alias("running_min_budget"),
        F.avg("budget").over(w_recent).alias("running_avg_budget"),
        F.row_number().over(w2).alias("seq_no"),
    )
)
