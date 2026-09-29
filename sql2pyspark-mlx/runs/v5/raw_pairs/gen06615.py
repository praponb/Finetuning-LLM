from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("revenue_per_unit", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("product_id").orderBy(F.col("revenue_per_unit").desc(), "id")
w3 = Window.orderBy("revenue_per_unit_avg_2_minus_revenue_per_unit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("revenue_per_unit_avg_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.orderBy("revenue_per_unit_avg_2", "id")

cleaned = (
    df_store_sales
    .filter(F.col("sale_date") > F.to_date(F.lit("2023-09-27")))
    .select(
        "id",
        "product_id",
        "sale_date",
        (F.col("units") + F.col("revenue")).alias("revenue_per_unit"),
    )
)

with_windows = (
    cleaned
    .withColumn("revenue_per_unit_avg_2", F.avg("revenue_per_unit").over(w))
    .withColumn("revenue_per_unit_bucket", F.ntile(5).over(w2))
)

filtered = (
    with_windows
    .filter((F.col("sale_date") > F.lit("2023-03-24").cast("date")) & (F.col("product_id") == 1))
    .select(
        "id",
        "sale_date",
        "revenue_per_unit",
        "revenue_per_unit_avg_2",
        "revenue_per_unit_bucket",
        (F.col("revenue_per_unit_avg_2") / F.nullif("revenue_per_unit", F.lit(0))).alias("revenue_per_unit_avg_2_per_revenue_per_unit"),
        (F.col("revenue_per_unit_avg_2") - F.col("revenue_per_unit")).alias("revenue_per_unit_avg_2_minus_revenue_per_unit"),
    )
)

scored = (
    filtered
    .select(
        "*",
        F.last("revenue_per_unit_avg_2", ignorenulls=True).over(w3).alias("last_known_revenue_per_unit_avg_2"),
        F.sum("revenue_per_unit_avg_2_minus_revenue_per_unit").over(w4).alias("cumulative_revenue_per_unit_avg_2_minus_revenue_per_unit"),
    )
)

base = (
    scored
    .filter(F.col("last_known_revenue_per_unit_avg_2").between(62, 70))
    .withColumn("sale_date_month_end", F.last_day("sale_date"))
    .withColumn("revenue_per_unit_avg_2_minus_revenue_per_unit_filled", F.coalesce("revenue_per_unit_avg_2_minus_revenue_per_unit", F.lit(0)))
)

scored_store_sales = (
    base
    .select(
        "id",
        "sale_date",
        "revenue_per_unit_avg_2",
        "revenue_per_unit_bucket",
        "cumulative_revenue_per_unit_avg_2_minus_revenue_per_unit",
        "revenue_per_unit_avg_2_minus_revenue_per_unit_filled",
        F.lead("revenue_per_unit_avg_2_minus_revenue_per_unit").over(w_running).alias("next_revenue_per_unit_avg_2_minus_revenue_per_unit"),
        F.lag("revenue_per_unit_avg_2_minus_revenue_per_unit").over(w_running).alias("prev_revenue_per_unit_avg_2_minus_revenue_per_unit"),
        F.lead("revenue_per_unit_avg_2_per_revenue_per_unit").over(w_running).alias("next_revenue_per_unit_avg_2_per_revenue_per_unit"),
        F.row_number().over(w_running).alias("row_seq"),
    )
)

result = (
    scored_store_sales
    .groupBy(F.date_trunc("WEEK", "sale_date").alias("week_start"))
    .agg(
        F.round(F.avg("revenue_per_unit_avg_2"), 2).alias("avg_revenue_per_unit_avg_2"),
        F.max("revenue_per_unit_bucket").alias("max_revenue_per_unit_bucket"),
        F.percentile_approx("next_revenue_per_unit_avg_2_per_revenue_per_unit", 0.95).alias("next_revenue_per_unit_avg_2_per_revenue_per_unit_p95"),
        F.count("*").alias("cnt"),
    )
    .orderBy(F.col("week_start").desc_nulls_last())
)
