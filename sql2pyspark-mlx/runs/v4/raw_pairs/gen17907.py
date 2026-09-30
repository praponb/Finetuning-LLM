from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("product_id", "store_id")
w3 = Window.partitionBy("store_id").orderBy(F.col("units").desc_nulls_first())
quantity_by_product_id = df_inventory.groupBy("product_id").agg(F.max("quantity").alias("max_inventory_quantity"))

scored = (
    df_store_sales
    .select(
        "*",
        F.last("revenue", ignorenulls=True).over(w).alias("last_known_revenue"),
        F.corr("revenue", "units").over(w2).alias("corr_revenue_units"),
        F.percent_rank().over(w3).alias("units_percent_rank"),
    )
)

matched_keys = (
    scored
    .select("store_id")
    .subtract(
        df_store_sales
        .filter(F.month("sale_date").isin([1, 4, 11]))
        .select("store_id")
    )
)

reconciled = (
    scored
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")), "inner")
    .select("s.*")
)

valid = (
    reconciled
    .filter(F.col("sale_date") >= F.lit("2024-03-10").cast("date"))
    .select(
        "*",
        F.round("last_known_revenue", 2).alias("last_known_revenue_rounded"),
        F.abs("corr_revenue_units").alias("abs_corr_revenue_units"),
    )
)

valid_store_sales = (
    valid
    .filter((F.col("sale_date") > F.lit("2024-03-17").cast("date")) & F.col("store_id").isin([2, 3, 5]))
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        "corr_revenue_units",
        "last_known_revenue_rounded",
        "abs_corr_revenue_units",
        (F.col("units") - F.col("revenue")).alias("revenue_per_unit"),
    )
)

stats = (
    valid_store_sales
    .groupBy("product_id")
    .agg(
        F.round(F.avg("corr_revenue_units"), 2).alias("avg_corr_revenue_units"),
        F.percentile_approx("revenue", [0.25, 0.5, 0.75]).alias("revenue_quartiles"),
        F.percentile_approx("last_known_revenue_rounded", 0.95).alias("last_known_revenue_rounded_p95"),
    )
)

result = (
    stats
    .join(quantity_by_product_id, on="product_id", how="left")
    .select(
        "product_id",
        "avg_corr_revenue_units",
        "revenue_quartiles",
        "last_known_revenue_rounded_p95",
        F.col("max_inventory_quantity"),
    )
    .orderBy("max_inventory_quantity")
)
