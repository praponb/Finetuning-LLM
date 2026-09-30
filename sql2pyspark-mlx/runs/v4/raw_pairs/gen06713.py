from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("units_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("cumulative_units_filled_minus_revenue", "id")
w4 = Window.orderBy(F.col("revenue").desc())
w5 = Window.orderBy("cumulative_units_filled_minus_revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_store_sales
    .filter(F.col("store_id").isin([1, 3, 5]))
    .select(
        "id",
        "sale_date",
        "units",
        "revenue",
        F.coalesce("units", F.lit(0)).alias("units_filled"),
    )
)

metrics = (
    base
    .withColumn("cumulative_units_filled", F.sum("units_filled").over(w))
    .withColumn("first_known_units", F.first("units", ignorenulls=True).over(w2))
)

filtered = (
    metrics
    .filter((F.col("sale_date") >= F.lit("2023-11-12").cast("date")) & (F.col("cumulative_units_filled") < 351))
    .select(
        "id",
        "sale_date",
        "units",
        "revenue",
        (F.col("cumulative_units_filled") - F.col("revenue")).alias("cumulative_units_filled_minus_revenue"),
    )
)

with_windows = (
    filtered
    .select(
        "*",
        F.ntile(3).over(w3).alias("cumulative_units_filled_minus_revenue_bucket"),
        F.rank().over(w4).alias("revenue_rank"),
        F.sum("cumulative_units_filled_minus_revenue").over(w5).alias("cumulative_cumulative_units_filled_minus_revenue"),
        F.sum("cumulative_units_filled_minus_revenue").over(w2).alias("running_cumulative_units_filled_minus_revenue"),
    )
)

report = (
    with_windows
    .select(
        "id",
        "sale_date",
        "revenue",
        "cumulative_units_filled_minus_revenue",
        "cumulative_cumulative_units_filled_minus_revenue",
    )
)

result = (
    report
    .select(
        "id",
        "revenue",
        "cumulative_units_filled_minus_revenue",
        "cumulative_cumulative_units_filled_minus_revenue",
    )
)
