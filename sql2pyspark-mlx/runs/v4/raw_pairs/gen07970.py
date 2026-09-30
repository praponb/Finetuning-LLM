from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id")
w_recent = Window.orderBy("revenue", "id")
w_grp = Window.partitionBy("product_id").orderBy("units_rounded", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("store_id")
w5 = Window.partitionBy("product_id").orderBy(F.col("revenue_minus_prev_units").desc_nulls_first(), "id")
w6 = Window.partitionBy("product_id").orderBy("store_id_min_revenue_share", "id")

base = (
    df_store_sales
    .filter((F.col("sale_date") >= F.to_date(F.lit("2023-05-07"))) & (F.col("units") <= 38))
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        F.last_day("sale_date").alias("sale_date_month_end"),
        F.round("units", 0).alias("units_rounded"),
    )
)

with_windows = (
    base
    .withColumn("revenue_share", F.round((F.col("revenue") / F.sum("revenue").over(w)), 4))
    .withColumn("prev_units", F.lag("units", 1).over(w_recent))
    .withColumn("first_known_units_rounded", F.first("units_rounded", ignorenulls=True).over(w_grp))
)

scored = (
    with_windows
    .select(
        "*",
        F.min("revenue_share").over(w4).alias("store_id_min_revenue_share"),
        F.stddev_samp("units_rounded").over(w4).alias("store_id_stddev_samp_units_rounded"),
    )
)

filtered = (
    scored
    .filter(F.col("product_id").isin([4, 5, 6, 8]))
    .withColumn("revenue_minus_prev_units", (F.col("revenue") - F.col("prev_units")))
)

ranked = (
    filtered
    .withColumn("dedup_rank", F.row_number().over(w5))
    .filter(F.col("dedup_rank") == 1)
)

ranked_store_sales = (
    ranked
    .withColumn("latest_rank", F.row_number().over(w6))
    .filter(F.col("latest_rank") == 1)
)

result = (
    ranked_store_sales
    .select(
        "id",
        "store_id",
        "revenue",
        F.col("units_rounded").alias("UNITS_ROUNDED"),
        F.col("revenue_minus_prev_units").alias("REVENUE_MINUS_PREV_UNITS"),
        "revenue_share",
    )
    .unpivot(
        ["id", "store_id"],
        ["revenue", "UNITS_ROUNDED", "REVENUE_MINUS_PREV_UNITS", "revenue_share"],
        "metric_name",
        "val",
    )
    .filter(F.col("val").isNotNull())
)
