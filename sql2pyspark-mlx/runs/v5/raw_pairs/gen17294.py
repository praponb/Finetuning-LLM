from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("location")
w3 = Window.partitionBy("dept_name").orderBy(F.col("cumulative_budget").desc())
w4 = Window.orderBy("cumulative_budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("dept_name").orderBy(F.col("cumulative_budget").desc(), "id")
w6 = Window.partitionBy("dept_name").orderBy(F.col("cumulative_budget_minus_location_max_budget").asc_nulls_first())
w7 = Window.orderBy(F.col("dept_name_length").desc(), "id")
w8 = Window.partitionBy("dept_name").orderBy("cumulative_budget_minus_location_max_budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_windows = (
    df_departments
    .filter(F.col("budget").between(2678000, 3439000))
    .select(
        "id",
        "dept_name",
        F.sum("budget").over(w).alias("cumulative_budget"),
        F.max("budget").over(w2).alias("location_max_budget"),
    )
)

valid = (
    with_windows
    .filter(F.col("dept_name").isNotNull())
    .withColumn("dept_name_length", F.length("dept_name"))
    .withColumn("cumulative_budget_minus_location_max_budget", (F.col("cumulative_budget") - F.col("location_max_budget")))
)

latest = (
    valid
    .withColumn("latest_rank", F.rank().over(w3))
    .filter(F.col("latest_rank") == 1)
)

with_windows_departments = (
    latest
    .select(
        "*",
        F.last("location_max_budget", ignorenulls=True).over(w4).alias("last_known_location_max_budget"),
        F.row_number().over(w5).alias("row_seq"),
    )
)

result = (
    with_windows_departments
    .select(
        "id",
        "dept_name",
        "location_max_budget",
        "dept_name_length",
        "cumulative_budget_minus_location_max_budget",
        "latest_rank",
        "row_seq",
        F.percent_rank().over(w6).alias("cumulative_budget_minus_location_max_budget_percent_rank"),
        F.row_number().over(w7).alias("event_seq"),
        F.last("dept_name", ignorenulls=True).over(w8).alias("last_known_dept_name"),
    )
)
