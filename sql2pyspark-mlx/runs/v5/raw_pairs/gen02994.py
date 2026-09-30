from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("units", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("product_id").orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("last7_min_least_units_revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("last7_count_least_units_revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("units", "id")
w7 = Window.orderBy("last7_min_least_units_revenue", "id")

valid = (
    df_store_sales
    .filter(F.col("sale_date") >= F.lit("2023-03-09").cast("date"))
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        F.least("units", "revenue").alias("least_units_revenue"),
    )
)

base = (
    valid
    .filter((F.col("sale_date") > F.lit("2023-08-19").cast("date")) & (F.col("sale_date") < "2025-10-01"))
    .withColumn("units_num", F.col("units").cast("string"))
)

metrics = (
    base
    .select(
        "id",
        "units",
        "least_units_revenue",
        "units_num",
        F.sum("least_units_revenue").over(w).alias("last7_sum_least_units_revenue"),
        F.count("least_units_revenue").over(w).alias("last7_count_least_units_revenue"),
        F.min("least_units_revenue").over(w).alias("last7_min_least_units_revenue"),
        F.last("units", ignorenulls=True).over(w2).alias("last_known_units"),
    )
)

metrics_store_sales = (
    metrics
    .filter(F.col("least_units_revenue") > 33)
    .select(
        "id",
        "least_units_revenue",
        "last7_sum_least_units_revenue",
        "last7_count_least_units_revenue",
        F.sum("last7_min_least_units_revenue").over(w3).alias("cumulative_last7_min_least_units_revenue"),
        F.sum("units").over(w4).alias("running_units"),
        F.first("last7_sum_least_units_revenue", ignorenulls=True).over(w5).alias("first_known_last7_sum_least_units_revenue"),
        F.lead("last7_count_least_units_revenue", 2).over(w6).alias("next_last7_count_least_units_revenue"),
        F.row_number().over(w7).alias("seq_no"),
    )
)

result = (
    metrics_store_sales
    .select(
        "id",
        "least_units_revenue",
        "cumulative_last7_min_least_units_revenue",
        "first_known_last7_sum_least_units_revenue",
        "next_last7_count_least_units_revenue",
    )
    .orderBy(F.col("cumulative_last7_min_least_units_revenue").desc_nulls_last(), "id")
    .limit(20)
)
