from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("dept_name").orderBy("budget", "id")

with_windows = (
    df_departments
    .filter(F.col("dept_name").isin(["Sales", "HR", "Engineering"]))
    .select(
        "id",
        "location",
        "budget",
        F.lag("budget").over(w_hist).alias("prev_budget"),
        F.lead("budget").over(w_hist).alias("next_budget"),
        F.row_number().over(w_hist).alias("seq_no"),
    )
)

result = (
    with_windows
    .select("id", "location", "budget", "prev_budget", "next_budget", "seq_no")
    .orderBy(F.col("location").asc_nulls_last())
)
