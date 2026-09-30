from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("units_adjusted", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("sale_date", "id")
w4 = Window.orderBy("running_count_units", "id")
w5 = Window.orderBy(F.col("last_known_units").desc_nulls_first())
w6 = Window.orderBy("running_avg_units", "id")

valid = (
    df_store_sales
    .filter(F.col("sale_date") >= F.to_date(F.lit("2023-09-19")))
    .select(
        "id",
        "sale_date",
        "units",
        F.round((F.col("units") * 1.07), 2).alias("units_adjusted"),
    )
)

cleaned = (
    valid
    .filter(F.col("units").between(17, 19))
    .withColumn("days_since_sale_date", F.datediff(F.current_date(), "sale_date"))
)

with_windows = (
    cleaned
    .select(
        "*",
        F.count("units").over(w_part).alias("running_count_units"),
        F.min("units").over(w_part).alias("running_min_units"),
        F.avg("units").over(w_part).alias("running_avg_units"),
        F.max("units").over(w_part).alias("running_max_units"),
        F.last("units", ignorenulls=True).over(w2).alias("last_known_units"),
    )
)

scored = (
    with_windows
    .select(
        "id",
        "sale_date",
        "units_adjusted",
        "running_count_units",
        "running_max_units",
        "last_known_units",
        F.lag("last_known_units", 1).over(w3).alias("prev_last_known_units"),
        F.lead("running_avg_units", 1).over(w4).alias("next_running_avg_units"),
        F.percent_rank().over(w5).alias("last_known_units_percent_rank"),
        F.lead("running_avg_units", 2).over(w6).alias("next_running_avg_units_2"),
    )
)

cleaned_store_sales = (
    scored
    .filter((F.col("sale_date") >= F.lit("2023-07-07").cast("date")) & (F.col("running_max_units") <= 40))
    .withColumn("running_max_units_whole", F.floor("running_max_units"))
    .withColumn("running_max_units_per_next_running_avg_units", F.round((F.col("running_max_units") / F.col("next_running_avg_units")), 2))
)

cleaned_store_sales_2 = (
    cleaned_store_sales
    .filter((F.col("sale_date") > F.lit("2023-05-09").cast("date")) & (F.col("last_known_units_percent_rank") <= 0.65))
    .withColumn("units_adjusted_times_running_max_units", (F.col("units_adjusted") * F.col("running_max_units")))
)

result = (
    cleaned_store_sales_2
    .select(
        "id",
        "units_adjusted",
        "running_count_units",
        "running_max_units",
        "last_known_units",
        "next_running_avg_units",
        "last_known_units_percent_rank",
        "running_max_units_per_next_running_avg_units",
        "units_adjusted_times_running_max_units",
    )
)
