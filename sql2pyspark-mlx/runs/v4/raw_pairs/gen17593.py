from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.orderBy("units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("store_id", "product_id")
w3 = Window.partitionBy("product_id").orderBy(F.col("store_id_avg_revenue_filled").desc_nulls_last())
w4 = Window.orderBy("revenue_rounded_times_store_id_avg_revenue_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_store_sales
    .filter(F.col("sale_date") > F.lit("2023-06-29").cast("date"))
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        F.round("revenue", 0).alias("revenue_rounded"),
        F.coalesce("revenue", F.lit(0)).alias("revenue_filled"),
    )
)

enriched = (
    cleaned
    .select(
        "*",
        F.first("revenue_rounded", ignorenulls=True).over(w_hist).alias("first_known_revenue_rounded"),
        F.avg("revenue_filled").over(w_part).alias("store_id_avg_revenue_filled"),
    )
)

cleaned_store_sales = (
    enriched
    .filter(F.col("revenue_filled") >= 38)
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "revenue_rounded",
        "store_id_avg_revenue_filled",
        (F.col("revenue_rounded") * F.col("store_id_avg_revenue_filled")).alias("revenue_rounded_times_store_id_avg_revenue_filled"),
    )
)

result = (
    cleaned_store_sales
    .select(
        "*",
        F.cume_dist().over(w3).alias("store_id_avg_revenue_filled_cume_dist"),
        F.sum("revenue_rounded").over(w4).alias("cumulative_revenue_rounded"),
    )
)
