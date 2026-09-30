from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("revenue", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.orderBy("revenue_per_unit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("running_least_revenue_per_unit_units", "id")
w5 = Window.orderBy("least_revenue_per_unit_units_avg_3", "id").rowsBetween(-3, Window.currentRow)
w6 = Window.orderBy("first_known_revenue", "id")

cleaned = (
    df_store_sales
    .filter(F.col("sale_date") >= F.lit("2023-07-13").cast("date"))
    .select(
        "id",
        "product_id",
        "units",
        "revenue",
        (F.col("units") + F.col("revenue")).alias("revenue_per_unit"),
    )
)

cleaned_store_sales = (
    cleaned
    .filter(F.col("revenue_per_unit") <= 56)
    .select(
        "id",
        "units",
        "revenue",
        "revenue_per_unit",
        F.least("revenue_per_unit", "units").alias("least_revenue_per_unit_units"),
    )
)

with_windows = (
    cleaned_store_sales
    .select(
        "id",
        "units",
        F.avg("least_revenue_per_unit_units").over(w).alias("least_revenue_per_unit_units_avg_3"),
        F.sum("revenue").over(w2).alias("running_revenue"),
        F.sum("least_revenue_per_unit_units").over(w3).alias("running_least_revenue_per_unit_units"),
        F.first("revenue", ignorenulls=True).over(w2).alias("first_known_revenue"),
    )
)

cleaned_store_sales_2 = (
    with_windows
    .filter(F.col("running_revenue") == 6935)
    .select(
        "*",
        F.round("running_least_revenue_per_unit_units", 2).alias("running_least_revenue_per_unit_units_rounded"),
        F.col("running_least_revenue_per_unit_units").cast("double").alias("running_least_revenue_per_unit_units_str"),
    )
)

valid = (
    cleaned_store_sales_2
    .filter(F.col("running_least_revenue_per_unit_units") >= 386)
    .withColumn("units_str", F.col("units").cast("string"))
)

with_windows_store_sales = (
    valid
    .filter(F.col("least_revenue_per_unit_units_avg_3").isNull())
    .select(
        "id",
        "units",
        "running_revenue",
        "first_known_revenue",
        "units_str",
        F.row_number().over(w4).alias("event_seq"),
        F.avg("running_least_revenue_per_unit_units_str").over(w5).alias("running_least_revenue_per_unit_units_str_avg_3"),
        F.lag("running_least_revenue_per_unit_units_rounded", 1).over(w6).alias("prev_running_least_revenue_per_unit_units_rounded"),
    )
)

result = (
    with_windows_store_sales
    .select(
        "id",
        "units",
        "running_revenue",
        "units_str",
        "event_seq",
        "running_least_revenue_per_unit_units_str_avg_3",
        "prev_running_least_revenue_per_unit_units_rounded",
    )
    .orderBy(F.col("running_least_revenue_per_unit_units_str_avg_3").asc_nulls_first())
)
