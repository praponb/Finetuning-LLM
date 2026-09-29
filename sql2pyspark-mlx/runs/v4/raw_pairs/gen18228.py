from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name")
w2 = Window.partitionBy("dept_name", "location").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("location")
w4 = Window.partitionBy("location").orderBy("budget_adjusted", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("location").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("location").orderBy("dept_name_count_location_length_filled_adjusted", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.orderBy("running_dept_name_count_location_length_filled_adjusted", "id")
w8 = Window.partitionBy("location").orderBy("running_budget")
w9 = Window.partitionBy("location").orderBy("dept_name_count_location_length_filled_adjusted")
w10 = Window.partitionBy("location").orderBy("running_location_count_dept_name_count_location_length_filled", "id")
w11 = Window.partitionBy("location").orderBy("dept_name_count_location_length_filled_adjusted", "id")
w12 = Window.partitionBy("location").orderBy(F.col("location_sum_location_count_dept_name_count_location_length_filled").asc_nulls_first())

valid = (
    df_departments
    .filter(F.col("location").isNotNull() & F.col("location").isin(["Bangkok", "New York"]))
    .withColumn("location_length", F.length("location"))
    .withColumn("budget_adjusted", F.round((F.col("budget") * 1.07), 2))
)

with_windows = (
    valid
    .select(
        "*",
        F.count("location_length").over(w).alias("dept_name_count_location_length"),
        F.last("budget_adjusted", ignorenulls=True).over(w2).alias("last_known_budget_adjusted"),
    )
)

valid_departments = (
    with_windows
    .filter(F.col("dept_name").isin(["HR", "Legal", "Marketing"]))
    .select(
        "id",
        "location",
        "budget",
        "location_length",
        "budget_adjusted",
        "last_known_budget_adjusted",
        F.coalesce("dept_name_count_location_length", F.lit(0)).alias("dept_name_count_location_length_filled"),
    )
)

cleaned = (
    valid_departments
    .filter(F.col("location").isin(["London", "Singapore", "Tokyo"]))
    .withColumn("dept_name_count_location_length_filled_adjusted", F.round((F.col("dept_name_count_location_length_filled") * 1.07), 2))
    .withColumn("resolved_location", F.coalesce("location", F.lit("N/A")))
)

scored = (
    cleaned
    .withColumn("location_count_dept_name_count_location_length_filled", F.count("dept_name_count_location_length_filled").over(w3))
    .withColumn("running_dept_name_count_location_length_filled_adjusted", F.sum("dept_name_count_location_length_filled_adjusted").over(w4))
)

with_windows_departments = (
    scored
    .withColumn("last_known_location", F.last("location", ignorenulls=True).over(w_ordered))
    .withColumn("running_budget", F.sum("budget").over(w6))
    .withColumn("running_location_count_dept_name_count_location_length_filled", F.sum("location_count_dept_name_count_location_length_filled").over(w_ordered))
    .withColumn("prev_location_count_dept_name_count_location_length_filled", F.lag("location_count_dept_name_count_location_length_filled", 1, 0).over(w_hist))
)

metrics = (
    with_windows_departments
    .select(
        "*",
        F.rank().over(w8).alias("running_budget_rank"),
        F.dense_rank().over(w9).alias("dept_name_count_location_length_filled_adjusted_dense_rank"),
        F.row_number().over(w10).alias("event_seq"),
        F.count("budget").over(w3).alias("location_count_budget"),
        F.sum("location_count_dept_name_count_location_length_filled").over(w3).alias("location_sum_location_count_dept_name_count_location_length_filled"),
    )
)

result = (
    metrics
    .select(
        "*",
        F.lag("dept_name_count_location_length_filled_adjusted_dense_rank", 2, 0).over(w11).alias("prev_dept_name_count_location_length_filled_adjusted_dense_rank"),
        F.percent_rank().over(w12).alias("location_sum_location_count_dept_name_count_location_length_filled_percent_rank"),
    )
)
