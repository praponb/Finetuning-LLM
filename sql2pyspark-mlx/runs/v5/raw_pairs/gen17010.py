from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id")

base = (
    df_store_sales
    .filter((F.col("sale_date") > F.lit("2023-02-17").cast("date")) & F.col("revenue").between(2400, 3200))
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        F.last_day("sale_date").alias("sale_date_month_end"),
        F.add_months("sale_date", 6).alias("sale_date_plus_months"),
    )
)

with_dims = (
    base
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id") == F.col("p.id"), "left")
    .filter(F.col("p.launch_date") < "2024-09-08")
    .select("s.*", F.col("p.cost"))
)

matched_keys = (
    with_dims
    .select("product_id")
    .subtract(
        df_store_sales
        .filter(F.col("product_id").isin([2, 5, 9, 10]))
        .select("product_id")
    )
)

kept_rows = (
    with_dims
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

with_windows = (
    kept_rows
    .select(
        "product_id",
        "sale_date",
        "units",
        "revenue",
        "cost",
        F.round((F.col("units") / F.sum("units").over(w)), 4).alias("units_share"),
        F.round((F.col("revenue") / F.sum("revenue").over(w)), 4).alias("revenue_share"),
        F.corr("units", "revenue").over(w).alias("corr_units_revenue"),
    )
)

grouped = (
    with_windows
    .rollup(
        F.when(F.col("units") >= 36, "HIGH").otherwise("LOW").alias("units_tier"),
        F.when(F.col("cost") >= 800, "HIGH").otherwise("LOW").alias("cost_tier"),
        F.when(F.col("corr_units_revenue") >= 0.45, "LARGE").otherwise("SMALL").alias("corr_units_revenue_tier"),
    )
    .agg(
        F.countDistinct("product_id").alias("distinct_product_ids"),
        F.round(F.avg("revenue"), 2).alias("avg_revenue"),
        F.percentile_approx("corr_units_revenue", 0.9).alias("corr_units_revenue_p90"),
    )
)

result = (
    grouped
    .select(
        "units_tier",
        "corr_units_revenue_tier",
        "distinct_product_ids",
        "avg_revenue",
        "corr_units_revenue_p90",
    )
)
