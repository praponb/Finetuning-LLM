from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("budget_filled", "id")
w2 = Window.partitionBy("location", "dept_name").orderBy("budget_filled", "id")
w3 = Window.orderBy("budget", "id").rowsBetween(-2, Window.currentRow)
w4 = Window.partitionBy("dept_name").orderBy("budget_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("dept_name", "location").orderBy("budget_filled", "id")

valid = (
    df_departments
    .filter(F.col("dept_name") == "Engineering")
    .select(
        "*",
        F.upper("dept_name").alias("dept_name_upper"),
        F.coalesce("budget", F.lit(0)).alias("budget_filled"),
        F.coalesce("dept_name", F.lit("UNKNOWN")).alias("resolved_dept_name"),
    )
)

result = (
    valid
    .filter(F.col("location").isin(["Bangkok", "Berlin", "London"]))
    .select(
        "id",
        "dept_name",
        "location",
        "budget",
        "dept_name_upper",
        "budget_filled",
        F.row_number().over(w).alias("row_seq"),
        F.lead("budget", 1).over(w2).alias("next_budget"),
        F.avg("budget_filled").over(w3).alias("budget_filled_avg_2"),
        F.sum("budget_filled").over(w4).alias("cumulative_budget_filled"),
        F.lag("budget_filled", 2).over(w5).alias("prev_budget_filled"),
    )
)
