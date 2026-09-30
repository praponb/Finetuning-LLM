from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy("revenue", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("store_id").orderBy("sale_date_month_end", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("store_id").orderBy("units", "id")
w4 = Window.partitionBy("store_id", "product_id")
w_recent = Window.orderBy("next_revenue", "id")

filtered = (
    df_store_sales
    .filter(F.col("sale_date") >= F.lit("2023-09-25").cast("date"))
    .select("*", F.last_day("sale_date").alias("sale_date_month_end"))
)

enriched = (
    filtered
    .select(
        "id",
        "sale_date",
        "units",
        "revenue",
        F.avg("units").over(w).alias("units_avg_6"),
        F.first("revenue", ignorenulls=True).over(w2).alias("first_known_revenue"),
        F.lead("revenue", 1).over(w3).alias("next_revenue"),
        F.corr("revenue", "units").over(w4).alias("corr_revenue_units"),
    )
)

valid = (
    enriched
    .filter(F.col("revenue") <= 3300)
    .select("*", (F.col("revenue") - F.col("units")).alias("revenue_per_unit"))
)

with_windows = (
    valid
    .select(
        "id",
        "units",
        "revenue",
        "first_known_revenue",
        "corr_revenue_units",
        "revenue_per_unit",
        F.lead("units_avg_6").over(w_recent).alias("next_units_avg_6"),
        F.lag("units_avg_6").over(w_recent).alias("prev_units_avg_6"),
        F.lead("first_known_revenue").over(w_recent).alias("next_first_known_revenue"),
        F.lag("first_known_revenue").over(w_recent).alias("prev_first_known_revenue"),
        F.row_number().over(w_recent).alias("row_seq"),
    )
)

totals = (
    with_windows
    .withColumn("prev_first_known_revenue_tier", F.when(F.col("prev_first_known_revenue") >= 3500, "HIGH").otherwise("LOW"))
    .rollup("prev_first_known_revenue_tier")
    .agg(
        F.percentile_approx("first_known_revenue", 0.9).alias("first_known_revenue_p90"),
        F.round(F.avg("next_units_avg_6"), 2).alias("avg_next_units_avg_6"),
        F.bit_and("units").alias("units_bit_and"),
        F.grouping("prev_first_known_revenue_tier").alias("grouping_level"),
    )
)

result = (
    totals
    .select(
        "prev_first_known_revenue_tier",
        "first_known_revenue_p90",
        "avg_next_units_avg_6",
        "units_bit_and",
    )
)
