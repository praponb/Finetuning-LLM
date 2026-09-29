from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("least_units_revenue_minus_adjusted_units").asc_nulls_first(), "id")
w2 = Window.partitionBy("store_id")
w3 = Window.partitionBy("store_id").orderBy("least_units_revenue", "id")
w4 = Window.partitionBy("store_id").orderBy(F.col("least_units_revenue").desc(), "id")
w_ordered = Window.partitionBy("store_id").orderBy("units", "id").rowsBetween(-6, -1)
w6 = Window.partitionBy("store_id").orderBy("days_since_sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_store_sales
    .filter(F.col("sale_date") > F.lit("2023-08-07").cast("date"))
    .withColumn("adjusted_units", F.round((F.col("units") * 1.07), 2))
    .withColumn("least_units_revenue", F.least("units", "revenue"))
)

base = (
    filtered
    .filter(F.col("revenue") > 2000)
    .withColumn("least_units_revenue_minus_adjusted_units", (F.col("least_units_revenue") - F.col("adjusted_units")))
    .withColumn("days_since_sale_date", F.datediff(F.current_date(), "sale_date"))
)

ranked = (
    base
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select(
        "id",
        "store_id",
        "units",
        "least_units_revenue",
        "least_units_revenue_minus_adjusted_units",
        "days_since_sale_date",
    )
)

with_windows = (
    ranked
    .select(
        "*",
        F.avg("units").over(w2).alias("store_id_avg_units"),
        F.lead("days_since_sale_date", 1).over(w3).alias("next_days_since_sale_date"),
        F.ntile(4).over(w4).alias("least_units_revenue_bucket"),
        F.avg("least_units_revenue").over(w_ordered).alias("least_units_revenue_avg_6_prior"),
        F.sum("least_units_revenue_minus_adjusted_units").over(w6).alias("cumulative_least_units_revenue_minus_adjusted_units"),
    )
)

valid = (
    with_windows
    .filter(F.col("least_units_revenue_minus_adjusted_units") >= 27)
    .select(
        "*",
        F.coalesce("least_units_revenue_avg_6_prior", F.lit(0)).alias("least_units_revenue_avg_6_prior_filled"),
    )
)

result = (
    valid
    .select(
        "id",
        "least_units_revenue_minus_adjusted_units",
        "next_days_since_sale_date",
    )
)
