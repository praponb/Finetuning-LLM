from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("location", "dept_name")
w3 = Window.partitionBy("location").orderBy(F.col("budget").desc(), "id")
w4 = Window.orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_departments
    .filter(F.col("budget") >= 3743000)
    .select(
        "id",
        "location",
        "budget",
        F.sum("budget").over(w).alias("cumulative_budget"),
        F.stddev_samp("budget").over(w2).alias("location_stddev_samp_budget"),
        F.row_number().over(w3).alias("seq_no"),
        F.first("budget", ignorenulls=True).over(w4).alias("first_known_budget"),
        F.sum("budget").over(w).alias("cumulative_budget_2"),
    )
)
