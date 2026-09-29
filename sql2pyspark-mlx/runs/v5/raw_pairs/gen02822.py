from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("sale_date", "id").rowsBetween(-2, -1)
w_ordered = Window.partitionBy("store_id").orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.orderBy("sale_date", "id")
w4 = Window.orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scored = (
    df_store_sales
    .filter(F.col("units") != 12)
    .select(
        "id",
        "sale_date",
        "units",
        F.avg("revenue").over(w).alias("revenue_avg_2_prior"),
        F.sum("units").over(w_ordered).alias("cumulative_units"),
        F.lag("revenue", 2, 0).over(w_hist).alias("prev_revenue"),
        F.sum("units").over(w4).alias("cumulative_units_2"),
    )
)

filtered = (
    scored
    .filter(F.col("sale_date") > F.to_date(F.lit("2023-03-28")))
    .select(
        "id",
        "sale_date",
        "units",
        "revenue_avg_2_prior",
        "cumulative_units",
        "prev_revenue",
        "cumulative_units_2",
        F.floor("cumulative_units_2").alias("cumulative_units_2_whole"),
        (F.col("prev_revenue") - F.col("cumulative_units_2")).alias("prev_revenue_minus_cumulative_units_2"),
    )
)

valid = (
    filtered
    .filter(F.col("sale_date") >= "2024-05-01")
    .select(
        "id",
        "sale_date",
        "revenue_avg_2_prior",
        "cumulative_units",
        "prev_revenue",
        "cumulative_units_2",
        "cumulative_units_2_whole",
        "prev_revenue_minus_cumulative_units_2",
        F.coalesce("prev_revenue", F.lit(0)).alias("prev_revenue_filled"),
        F.date_trunc("MONTH", "sale_date").alias("sale_date_period"),
    )
)

valid_store_sales = (
    valid
    .filter(F.col("revenue_avg_2_prior") <= 1603)
    .withColumn("cumulative_units_2_whole_str", F.col("cumulative_units_2_whole").cast("string"))
    .withColumn("prev_revenue_times_prev_revenue_minus_cumulative_units_2", (F.col("prev_revenue") * F.col("prev_revenue_minus_cumulative_units_2")))
)

cleaned = (
    valid_store_sales
    .filter(F.col("sale_date") >= F.to_date(F.lit("2023-08-07")))
    .select(
        "id",
        "sale_date",
        "revenue_avg_2_prior",
        "cumulative_units",
        "prev_revenue",
        "cumulative_units_2_whole",
        "prev_revenue_minus_cumulative_units_2",
        "prev_revenue_filled",
        "sale_date_period",
        "prev_revenue_times_prev_revenue_minus_cumulative_units_2",
        F.add_months("sale_date", 6).alias("sale_date_plus_months"),
    )
)

totals = (
    cleaned
    .withColumn("cumulative_units_tier", F.when(F.col("cumulative_units") >= 217, "PREMIUM").otherwise("STANDARD"))
    .rollup("cumulative_units_tier")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.min("sale_date_period").alias("first_seen"),
        ((F.unix_timestamp(F.max("sale_date_period")) - F.unix_timestamp(F.min("sale_date_period"))) / 60).alias("duration_min"),
        F.round(F.avg("cumulative_units_2_whole"), 2).alias("avg_cumulative_units_2_whole"),
        F.grouping("cumulative_units_tier").alias("gid"),
    )
    .filter(F.col("duration_min") >= 0)
)

result = totals.select("first_seen", "duration_min", "gid")
